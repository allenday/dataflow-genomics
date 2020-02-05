package com.google.allenday.genomics.core.processing;

import com.google.allenday.genomics.core.model.*;
import com.google.allenday.genomics.core.processing.align.AlignTransform;
import com.google.allenday.genomics.core.processing.sam.CreateBamIndexFn;
import com.google.allenday.genomics.core.processing.sam.MergeFn;
import com.google.allenday.genomics.core.processing.sam.SortFn;
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

public class AlignAndPostProcessTransform extends PTransform<PCollection<KV<SampleMetaData, List<FileWrapper>>>,
        PCollection<KV<SraSampleIdReferencePair, BamWithIndexUris>>> {

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
    public PCollection<KV<SraSampleIdReferencePair, BamWithIndexUris>> expand(
            PCollection<KV<SampleMetaData, List<FileWrapper>>> input) {
        PCollection<KV<SraSampleIdReferencePair, FileWrapper>> mergedAlignedSequences = input
                .apply("Align reads transform", alignTransform)
                .apply("Sort aligned results", ParDo.of(sortFn))
                .apply("Prepare for merge", MapElements.via(new ToSraSampleRefKV()))
                .apply("Group by meta data and reference", GroupByKey.create())
                .apply("IterToList utils 2", new ValueIterableToValueListTransform<>())
                .apply("Merge aligned results", ParDo.of(mergeFn));
        PCollection<KV<SraSampleIdReferencePair, FileWrapper>> bamIndexes =
                mergedAlignedSequences.apply("Create BAM index", ParDo.of(createBamIndexFn));

        final TupleTag<FileWrapper> mergedAlignedSequencesTag = new TupleTag<>();
        final TupleTag<FileWrapper> bamIndexesTag = new TupleTag<>();

        return
                KeyedPCollectionTuple.of(mergedAlignedSequencesTag, mergedAlignedSequences)
                        .and(bamIndexesTag, bamIndexes)
                        .apply("Co-Group merged results and indexes", CoGroupByKey.create())
                        .apply("Prepare uris output", MapElements.via(new SimpleFunction<KV<SraSampleIdReferencePair, CoGbkResult>,
                                KV<SraSampleIdReferencePair, BamWithIndexUris>>() {
                            @Override
                            public KV<SraSampleIdReferencePair, BamWithIndexUris> apply(
                                    KV<SraSampleIdReferencePair, CoGbkResult> input) {
                                CoGbkResult coGbkResult = input.getValue();
                                FileWrapper mergedAlignedSequenceFileWrapper = coGbkResult.getOnly(mergedAlignedSequencesTag);
                                FileWrapper bamIndexFileWrapper = coGbkResult.getOnly(bamIndexesTag);
                                return KV.of(input.getKey(), new BamWithIndexUris(mergedAlignedSequenceFileWrapper.getBlobUri(),
                                        bamIndexFileWrapper.getBlobUri()));
                            }
                        }));
    }

    public static class ToSraSampleRefKV extends SimpleFunction<KV<KV<SampleMetaData, ReferenceDatabase>, FileWrapper>,
            KV<SraSampleIdReferencePair, FileWrapper>> {
        @Override
        public KV<SraSampleIdReferencePair, FileWrapper> apply(
                KV<KV<SampleMetaData, ReferenceDatabase>, FileWrapper> input) {
            SampleMetaData sampleMetaData = input.getKey().getKey();
            ReferenceDatabase referenceDatabase = input.getKey().getValue();
            return KV.of(new SraSampleIdReferencePair(sampleMetaData.getSraSample(), referenceDatabase), input.getValue());
        }
    }
}
