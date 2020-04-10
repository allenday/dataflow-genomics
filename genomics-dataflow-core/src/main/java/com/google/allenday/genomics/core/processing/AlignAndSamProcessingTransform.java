package com.google.allenday.genomics.core.processing;

import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.processing.align.AlignTransform;
import com.google.allenday.genomics.core.processing.index.CreateBamIndexFn;
import com.google.allenday.genomics.core.processing.merge.MergeFn;
import com.google.allenday.genomics.core.processing.split.SamIntoRegionBatchesFn;
import com.google.allenday.genomics.core.model.SamRecordsMetadaKey;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.allenday.genomics.core.utils.ValueIterableToValueListTransform;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Contains queue of genomics transformation namely
 * <a href="https://en.wikipedia.org/wiki/Sequence_alignment">Sequence alignment</a> (FASTQ->SAM),
 * converting to binary format (SAM->BAM), sorting FASTQ and merging FASTQ merging BAM in scope of specific <a href="https://en.wikipedia.org/wiki/Contig">contig</a> region
 */
public class AlignAndSamProcessingTransform extends PTransform<PCollection<KV<SampleMetaData, List<FileWrapper>>>,
        PCollection<KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, BamWithIndexUris>>>> {

    public AlignTransform alignTransform;
    public SamIntoRegionBatchesFn samIntoRegionBatchesFn;
    public MergeFn regionsMergeFn;
    public FinalMergeTransform finalMergeTransform;
    public CreateBamIndexFn createBamIndexFn;
    private boolean withFinalMerge;

    public AlignAndSamProcessingTransform(AlignTransform alignTransform,
                                          SamIntoRegionBatchesFn samIntoRegionBatchesFn,
                                          MergeFn regionsMergeFn,
                                          FinalMergeTransform finalMergeTransform,
                                          CreateBamIndexFn createBamIndexFn,
                                          boolean withFinalMerge) {
        this.alignTransform = alignTransform;
        this.samIntoRegionBatchesFn = samIntoRegionBatchesFn;
        this.regionsMergeFn = regionsMergeFn;
        this.finalMergeTransform = finalMergeTransform;
        this.createBamIndexFn = createBamIndexFn;
        this.withFinalMerge = withFinalMerge;
    }

    @Override
    public PCollection<KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, BamWithIndexUris>>> expand(
            PCollection<KV<SampleMetaData, List<FileWrapper>>> input) {
        PCollection<KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>>> mergedAlignedSequences = input
                .apply("Align reads transform", alignTransform)
                .apply("Split SAM file into regions files", ParDo.of(samIntoRegionBatchesFn))
                .apply("Group by meta data and reference", GroupByKey.create())
                .apply(new ValueIterableToValueListTransform<>())
                .apply("Prepare for Merge", ParDo.of(new PrepareForMergeFn<>()))
                .apply("Merge aligned results", ParDo.of(regionsMergeFn));

        if (withFinalMerge) {
            mergedAlignedSequences.apply("Output final merge results", finalMergeTransform);
        }
        PCollection<KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>>> bamIndexes =
                mergedAlignedSequences
                        .apply("Create BAM index", ParDo.of(createBamIndexFn));


        final TupleTag<KV<ReferenceDatabaseSource, FileWrapper>> mergedAlignedSequencesTag = new TupleTag<>();
        final TupleTag<KV<ReferenceDatabaseSource, FileWrapper>> bamIndexesTag = new TupleTag<>();

        return
                KeyedPCollectionTuple.of(mergedAlignedSequencesTag, mergedAlignedSequences)
                        .and(bamIndexesTag, bamIndexes)
                        .apply("Co-Group merged results and indexes", CoGroupByKey.create())
                        .apply("Prepare uris output", MapElements.via(new SimpleFunction<KV<SamRecordsMetadaKey, CoGbkResult>,
                                KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, BamWithIndexUris>>>() {
                            @Override
                            public KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, BamWithIndexUris>> apply(
                                    KV<SamRecordsMetadaKey, CoGbkResult> input) {
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

    public static class PrepareForMergeFn<T> extends DoFn<KV<T, List<KV<ReferenceDatabaseSource, FileWrapper>>>,
            KV<T, KV<ReferenceDatabaseSource, List<FileWrapper>>>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            T key = c.element().getKey();
            List<KV<ReferenceDatabaseSource, FileWrapper>> groppedList = c.element().getValue();

            groppedList.stream().findFirst().ifPresent(kv ->
                    c.output(KV.of(key, KV.of(kv.getKey(), groppedList.stream().map(KV::getValue).collect(Collectors.toList())))));

        }
    }


    public static class FinalMergeTransform extends PTransform<PCollection<KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>>>,
            PCollection<KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>>>> {

        private MergeFn finalMergeFn;

        public FinalMergeTransform(MergeFn finalMergeFn) {
            this.finalMergeFn = finalMergeFn;
        }

        @Override
        public PCollection<KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>>> expand(
                PCollection<KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>>> input) {
            return input.apply(MapElements
                    .into(TypeDescriptors.kvs(TypeDescriptor.of(SamRecordsMetadaKey.class),
                            TypeDescriptors.kvs(TypeDescriptor.of(ReferenceDatabaseSource.class), TypeDescriptor.of(FileWrapper.class))))
                    .via(kv -> KV.of(kv.getKey().cloneWithUndefinedRegion(), kv.getValue())))
                    .apply("Group by meta data and reference", GroupByKey.create())
                    .apply(new ValueIterableToValueListTransform<>())
                    .apply("Prepare for Merge", ParDo.of(new PrepareForMergeFn<>()))
                    .apply("Merge final", ParDo.of(finalMergeFn));
        }
    }

}
