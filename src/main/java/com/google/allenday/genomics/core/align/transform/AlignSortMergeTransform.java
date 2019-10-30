package com.google.allenday.genomics.core.align.transform;

import com.google.allenday.genomics.core.gene.FileWrapper;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.gene.GeneReadGroupMetaData;
import com.google.allenday.genomics.core.gene.ReferenceDatabase;
import com.google.allenday.genomics.core.utils.ValueIterableToValueListTransform;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.util.List;

public class AlignSortMergeTransform extends PTransform<PCollection<KV<GeneExampleMetaData, List<FileWrapper>>>,
        PCollection<KV<KV<GeneReadGroupMetaData, ReferenceDatabase>, FileWrapper>>> {

    public AlignFn alignFn;
    public SortFn sortFn;
    public MergeFn mergeFn;

    public AlignSortMergeTransform(@Nullable String name, AlignFn alignFn, SortFn sortFn, MergeFn mergeFn) {
        super(name);
        this.alignFn = alignFn;
        this.sortFn = sortFn;
        this.mergeFn = mergeFn;
    }

    @Override
    public PCollection<KV<KV<GeneReadGroupMetaData, ReferenceDatabase>, FileWrapper>> expand(PCollection<KV<GeneExampleMetaData, List<FileWrapper>>> input) {
        return input
                .apply(ParDo.of(alignFn))
                .apply(ParDo.of(sortFn))
                .apply(MapElements.via(new SimpleFunction<KV<KV<GeneExampleMetaData, ReferenceDatabase>, FileWrapper>, KV<KV<GeneReadGroupMetaData, ReferenceDatabase>, FileWrapper>>() {
                    @Override
                    public KV<KV<GeneReadGroupMetaData, ReferenceDatabase>, FileWrapper> apply(KV<KV<GeneExampleMetaData, ReferenceDatabase>, FileWrapper> input) {
                        GeneReadGroupMetaData geneReafdGroupMetaData = input.getKey().getKey();
                        ReferenceDatabase referenceDatabase = input.getKey().getValue();
                        return KV.of(KV.of(geneReafdGroupMetaData, referenceDatabase), input.getValue());
                    }
                }))
                .apply(GroupByKey.create())
                .apply("IterToList utils 2", new ValueIterableToValueListTransform<>())
                .apply(ParDo.of(mergeFn));
    }
}
