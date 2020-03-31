package com.google.allenday.genomics.core.processing;

import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraSampleIdReferencePair;
import com.google.allenday.genomics.core.processing.align.AlignTransform;
import com.google.allenday.genomics.core.processing.sam.CreateBamIndexFn;
import com.google.allenday.genomics.core.processing.sam.MergeFn;
import com.google.allenday.genomics.core.processing.sam.SortFn;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.allenday.genomics.core.utils.ValueIterableToValueListTransform;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Contains queue of genomics transformation namely
 * <a href="https://en.wikipedia.org/wiki/Sequence_alignment">Sequence alignment</a> (FASTQ->SAM),
 * converting to binary format (SAM->BAM), sorting FASTQ and merging FASTQ in scope of single sample
 */
public class AlignAndPostProcessTransform extends PTransform<PCollection<KV<SampleMetaData, List<FileWrapper>>>,
        PCollection<KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, BamWithIndexUris>>>> {

    public AlignTransform alignTransform;
    public SortFn sortFn;
    public MergeFn mergeFn;
    public CreateBamIndexFn createBamIndexFn;

    public AlignAndPostProcessTransform(@Nullable String name, AlignTransform alignTransform, SortFn sortFn,
                                        MergeFn mergeFn, CreateBamIndexFn createBamIndexFn) {
        super(name);
        this.alignTransform = alignTransform;
        this.sortFn = sortFn;
        this.mergeFn = mergeFn;
        this.createBamIndexFn = createBamIndexFn;
    }

    @Override
    public PCollection<KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, BamWithIndexUris>>> expand(
            PCollection<KV<SampleMetaData, List<FileWrapper>>> input) {
        PCollection<KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, FileWrapper>>> mergedAlignedSequences = input
                .apply("Align reads transform", alignTransform)
                .apply("Sort aligned results", ParDo.of(sortFn))
                .apply("Prepare for Group by sra and reference", MapElements.via(new ToSraSampleRefKV()))
                .apply("Group by meta data and reference", GroupByKey.create())
                .apply(new ValueIterableToValueListTransform<>())
                .apply("Prepare for Merge", ParDo.of(new PrepareForMergeFn()))
                .apply("Merge aligned results", ParDo.of(mergeFn));
        PCollection<KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, FileWrapper>>> bamIndexes =
                mergedAlignedSequences.apply("Create BAM index", ParDo.of(createBamIndexFn));

        final TupleTag<KV<ReferenceDatabaseSource, FileWrapper>> mergedAlignedSequencesTag = new TupleTag<>();
        final TupleTag<KV<ReferenceDatabaseSource, FileWrapper>> bamIndexesTag = new TupleTag<>();

        return
                KeyedPCollectionTuple.of(mergedAlignedSequencesTag, mergedAlignedSequences)
                        .and(bamIndexesTag, bamIndexes)
                        .apply("Co-Group merged results and indexes", CoGroupByKey.create())
                        .apply("Prepare uris output", MapElements.via(new SimpleFunction<KV<SraSampleIdReferencePair, CoGbkResult>,
                                KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, BamWithIndexUris>>>() {
                            @Override
                            public KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, BamWithIndexUris>> apply(
                                    KV<SraSampleIdReferencePair, CoGbkResult> input) {
                                CoGbkResult coGbkResult = input.getValue();
                                KV<ReferenceDatabaseSource, FileWrapper> mergedAlignedSequenceFileWrapper = coGbkResult.getOnly(mergedAlignedSequencesTag);
                                KV<ReferenceDatabaseSource, FileWrapper> bamIndexFileWrapper = coGbkResult.getOnly(bamIndexesTag);

                                ReferenceDatabaseSource referenceDatabaseSource = mergedAlignedSequenceFileWrapper.getKey();
                                return KV.of(input.getKey(),
                                        KV.of(referenceDatabaseSource,
                                                new BamWithIndexUris(mergedAlignedSequenceFileWrapper.getValue().getBlobUri(),
                                                        bamIndexFileWrapper.getValue().getBlobUri())));
                            }
                        }));
    }

    public static class ToSraSampleRefKV extends SimpleFunction<KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>,
            KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, FileWrapper>>> {
        @Override
        public KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, FileWrapper>> apply(
                KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>> input) {
            SampleMetaData sampleMetaData = input.getKey();
            ReferenceDatabaseSource referenceDatabaseSource = input.getValue().getKey();
            FileWrapper fileWrapper = input.getValue().getValue();
            return KV.of(new SraSampleIdReferencePair(sampleMetaData.getSraSample(), referenceDatabaseSource.getName()),
                    input.getValue());
        }
    }

    public static class PrepareForMergeFn extends DoFn<KV<SraSampleIdReferencePair, List<KV<ReferenceDatabaseSource, FileWrapper>>>,
            KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, List<FileWrapper>>>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            SraSampleIdReferencePair sraSampleIdReferencePair = c.element().getKey();
            List<KV<ReferenceDatabaseSource, FileWrapper>> groppedList = c.element().getValue();

            groppedList.stream().findFirst().ifPresent(kv -> {
                c.output(KV.of(sraSampleIdReferencePair, KV.of(kv.getKey(), groppedList.stream().map(KV::getValue).collect(Collectors.toList()))));
            });

        }
    }
}
