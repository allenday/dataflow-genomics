package com.google.allenday.genomics.core.processing;

import com.google.allenday.genomics.core.model.*;
import com.google.allenday.genomics.core.processing.align.AlignTransform;
import com.google.allenday.genomics.core.processing.other.CreateBamIndexFn;
import com.google.allenday.genomics.core.processing.other.MergeFn;
import com.google.allenday.genomics.core.processing.other.SortFn;
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
        PCollection<KV<KV<ReadGroupMetaData, ReferenceDatabase>, BamWithIndexUris>>> {

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
    public PCollection<KV<KV<ReadGroupMetaData, ReferenceDatabase>, BamWithIndexUris>> expand(
            PCollection<KV<SampleMetaData, List<FileWrapper>>> input) {
        PCollection<KV<KV<ReadGroupMetaData, ReferenceDatabase>, FileWrapper>> mergedAlignedSequences = input
                .apply("Align reads transform", alignTransform)
                .apply("Sort aligned results", ParDo.of(sortFn))
                .apply("Prepare for merge", MapElements.via(new SimpleFunction<KV<KV<SampleMetaData, ReferenceDatabase>, FileWrapper>,
                        KV<KV<ReadGroupMetaData, ReferenceDatabase>, FileWrapper>>() {
                    @Override
                    public KV<KV<ReadGroupMetaData, ReferenceDatabase>, FileWrapper> apply(
                            KV<KV<SampleMetaData, ReferenceDatabase>, FileWrapper> input) {
                        ReadGroupMetaData geneReafdGroupMetaData = input.getKey().getKey();
                        ReferenceDatabase referenceDatabase = input.getKey().getValue();
                        return KV.of(KV.of(geneReafdGroupMetaData, referenceDatabase), input.getValue());
                    }
                }))
                .apply("Group by meta data and reference", GroupByKey.create())
                .apply("IterToList utils 2", new ValueIterableToValueListTransform<>())
                .apply("Merge aligned results", ParDo.of(mergeFn));
        PCollection<KV<KV<ReadGroupMetaData, ReferenceDatabase>, FileWrapper>> bamIndexes =
                mergedAlignedSequences.apply("Create BAM index", ParDo.of(createBamIndexFn));

        final TupleTag<FileWrapper> mergedAlignedSequencesTag = new TupleTag<>();
        final TupleTag<FileWrapper> bamIndexesTag = new TupleTag<>();

        return
                KeyedPCollectionTuple.of(mergedAlignedSequencesTag, mergedAlignedSequences)
                        .and(bamIndexesTag, bamIndexes)
                        .apply("Co-Group merged results and indexes", CoGroupByKey.create())
                        .apply("Prepare uris output", MapElements.via(new SimpleFunction<KV<KV<ReadGroupMetaData, ReferenceDatabase>, CoGbkResult>,
                                KV<KV<ReadGroupMetaData, ReferenceDatabase>, BamWithIndexUris>>() {
                            @Override
                            public KV<KV<ReadGroupMetaData, ReferenceDatabase>, BamWithIndexUris> apply(
                                    KV<KV<ReadGroupMetaData, ReferenceDatabase>, CoGbkResult> input) {
                                CoGbkResult coGbkResult = input.getValue();
                                FileWrapper mergedAlignedSequenceFileWrapper = coGbkResult.getOnly(mergedAlignedSequencesTag);
                                FileWrapper bamIndexFileWrapper = coGbkResult.getOnly(bamIndexesTag);
                                return KV.of(input.getKey(), new BamWithIndexUris(mergedAlignedSequenceFileWrapper.getBlobUri(),
                                        bamIndexFileWrapper.getBlobUri()));
                            }
                        }));
    }
}
