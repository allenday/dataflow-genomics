package com.google.allenday.genomics.core.preparing;

import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.metadata.EnrichWithFastqRunInputResourceFn;
import com.google.allenday.genomics.core.preparing.metadata.EnrichWithSraInputResourceFn;
import com.google.allenday.genomics.core.preparing.metadata.ParseMetadataAndFilterTr;
import com.google.allenday.genomics.core.preparing.runfile.FastqInputResource;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.preparing.fastq.BuildFastqContentFn;
import com.google.allenday.genomics.core.preparing.fastq.ReadFastqAndSplitIntoChunksFn;
import com.google.allenday.genomics.core.pipeline.transform.BreakFusion;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Provides a queue of data preparing transformations.
 * It includes reading input CSV file, parsing, filtering, anomalies and broken data check, splitting into smaller runfile chunks.
 * Return ready to post-process (e.g. sequence aligning) key-value pair of {@link SampleRunMetaData} and
 * list of {@link FileWrapper} (with 1 element for single end read and 2 elements - for paired end reads)
 */
public class RetrieveFastqFromCsvTransform extends PTransform<PBegin, PCollection<KV<SampleRunMetaData, List<FileWrapper>>>> {

    private String csvGcsUri;
    private SampleRunMetaData.Parser csvParser;

    private EnrichWithFastqRunInputResourceFn enrichWithFastqRunInputResourceFn;
    private EnrichWithSraInputResourceFn enrichWithSraInputResourceFn;


    private ReadFastqAndSplitIntoChunksFn.FromFastqInputResource fromFastqInputResourceTransform;
    private ReadFastqAndSplitIntoChunksFn.FromSraInputResource fromSraInputResourceTransform;
    private BuildFastqContentFn buildFastqContentFn;

    private boolean hasFastqChunkByteSizeLimitation;

    private List<String> sraSamplesToFilter;
    private List<String> sraSamplesToSkip;
    private PTransform<PCollection<KV<SampleRunMetaData, List<FastqInputResource>>>,
            PCollection<KV<SampleRunMetaData, List<FastqInputResource>>>> preparingTransforms;

    public RetrieveFastqFromCsvTransform(String csvGcsUri,
                                         SampleRunMetaData.Parser csvParser,
                                         EnrichWithFastqRunInputResourceFn enrichWithFastqRunInputResourceFn,
                                         EnrichWithSraInputResourceFn enrichWithSraInputResourceFn,
                                         ReadFastqAndSplitIntoChunksFn.FromFastqInputResource fromFastqInputResourceTransform,
                                         ReadFastqAndSplitIntoChunksFn.FromSraInputResource fromSraInputResourceTransform,
                                         BuildFastqContentFn buildFastqContentFn,
                                         boolean hasFastqChunkByteSizeLimitation) {
        this.csvGcsUri = csvGcsUri;
        this.csvParser = csvParser;
        this.enrichWithFastqRunInputResourceFn = enrichWithFastqRunInputResourceFn;
        this.enrichWithSraInputResourceFn = enrichWithSraInputResourceFn;
        this.fromFastqInputResourceTransform = fromFastqInputResourceTransform;
        this.fromSraInputResourceTransform = fromSraInputResourceTransform;
        this.buildFastqContentFn = buildFastqContentFn;
        this.hasFastqChunkByteSizeLimitation = hasFastqChunkByteSizeLimitation;
    }


    public RetrieveFastqFromCsvTransform withSraSamplesToFilter(List<String> sraSamplesToFilter) {
        this.sraSamplesToFilter = sraSamplesToFilter;
        return this;
    }

    public RetrieveFastqFromCsvTransform withSraSamplesToSkip(List<String> sraSamplesToSkip) {
        this.sraSamplesToSkip = sraSamplesToSkip;
        return this;
    }

    public RetrieveFastqFromCsvTransform withPreparingTransforms(
            PTransform<PCollection<KV<SampleRunMetaData, List<FastqInputResource>>>,
                    PCollection<KV<SampleRunMetaData, List<FastqInputResource>>>> preparingTransforms) {
        this.preparingTransforms = preparingTransforms;
        return this;
    }

    private ParseMetadataAndFilterTr createParseMetadataCsvIntoSampleMetadataTransform
            (List<SampleRunMetaData.DataSource.Type> dataSourceTypesToFilter) {
        return new ParseMetadataAndFilterTr(csvGcsUri,
                csvParser,
                dataSourceTypesToFilter,
                sraSamplesToFilter,
                sraSamplesToSkip);
    }

    @Override
    public PCollection<KV<SampleRunMetaData, List<FileWrapper>>> expand(PBegin input) {
        PCollection<KV<SampleRunMetaData, KV<FileWrapper, Integer>>> fastqChanksFromFastqInputResource =
                input.apply(createParseMetadataCsvIntoSampleMetadataTransform(SampleRunMetaData.DataSource.Type.allExceptSra()))
                        .apply(ParDo.of(enrichWithFastqRunInputResourceFn))
                        .apply(preparingTransforms)
                        .apply("Split pairs files into separate threads", FlatMapElements
                                .via(new IntoIndexedKvValues<>()))
                        .apply("Group by fastq URL", GroupByKey.create())
                        .apply(MapElements.via(new RestructureIndexedKV<>()))
                        .apply(ParDo.of(fromFastqInputResourceTransform));

        PCollection<KV<SampleRunMetaData, KV<FileWrapper, Integer>>> fastqChanksFromSraInputResource =
                input.apply(createParseMetadataCsvIntoSampleMetadataTransform(SampleRunMetaData.DataSource.Type.onlySra()))
                        .apply(ParDo.of(enrichWithSraInputResourceFn))
                        .apply(ParDo.of(fromSraInputResourceTransform));

        PCollection<KV<SampleRunMetaData, Iterable<KV<FileWrapper, Integer>>>> grouppedByChunks = PCollectionList.of(fastqChanksFromFastqInputResource).and(fastqChanksFromSraInputResource).apply(Flatten.pCollections())
                .apply("Group by part index", GroupByKey.create());
        if (hasFastqChunkByteSizeLimitation) {
            return grouppedByChunks
                    .apply(MapElements.via(new ToTheSamePartIndexFn()))
                    .apply("Group group chunks by original runfile", GroupByKey.create())
                    .apply(ParDo.of(buildFastqContentFn))
                    .apply(BreakFusion.create());
        } else {
            return grouppedByChunks.apply(MapElements
                    .via(new SortIndexedKV<>()));
        }
    }

    public static class SortIndexedKV<K, V> extends SimpleFunction<KV<K, Iterable<KV<V, Integer>>>, KV<K, List<V>>> {
        @Override
        public KV<K, List<V>> apply(KV<K, Iterable<KV<V, Integer>>> input) {
            List<V> values =
                    StreamSupport.stream(input.getValue().spliterator(), false)
                            .sorted(Comparator.comparing(KV::getValue)).map(KV::getKey).collect(Collectors.toList());
            return KV.of(input.getKey(), values);
        }
    }

    public static class IntoIndexedKvValues<T1, T2> extends InferableFunction<KV<T1, List<T2>>,
            Iterable<KV<KV<T1, Integer>, T2>>> {
        @Override
        public Iterable<KV<KV<T1, Integer>, T2>>
        apply(KV<T1, List<T2>> input) {
            List<KV<KV<T1, Integer>, T2>> output = new ArrayList<>();

            IntStream.range(0, input.getValue().size())
                    .forEach(index -> {
                        T2 t2 = input.getValue().get(index);
                        output.add(KV.of(KV.of(input.getKey(), index), t2));
                    });
            return output;
        }
    }

    public static class RestructureIndexedKV<T1, T2, T3> extends SimpleFunction<KV<KV<T1, T2>, Iterable<T3>>, KV<T1, KV<T3, T2>>> {
        @Override
        public KV<T1, KV<T3, T2>> apply(KV<KV<T1, T2>, Iterable<T3>> input) {
            T1 t1 = input.getKey().getKey();
            T2 t2 = input.getKey().getValue();
            T3 t3 = input.getValue().iterator().next();
            return KV.of(t1, KV.of(t3, t2));
        }
    }

    public static class ToTheSamePartIndexFn extends SimpleFunction<KV<SampleRunMetaData, Iterable<KV<FileWrapper, Integer>>>,
            KV<SampleRunMetaData, Iterable<KV<FileWrapper, Integer>>>> {
        @Override
        public KV<SampleRunMetaData, Iterable<KV<FileWrapper, Integer>>> apply(KV<SampleRunMetaData, Iterable<KV<FileWrapper, Integer>>> input) {
            SampleRunMetaData sampleRunMetaData = input.getKey();
            Iterable<KV<FileWrapper, Integer>> value = input.getValue();
            return KV.of(sampleRunMetaData.cloneWithNewPartIndex(0), value);
        }
    }
}
