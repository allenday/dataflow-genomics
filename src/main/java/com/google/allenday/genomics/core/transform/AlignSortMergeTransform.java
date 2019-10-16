package com.google.allenday.genomics.core.transform;

import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.gene.GeneReadGroupMetaData;
import com.google.allenday.genomics.core.transform.fn.AlignFn;
import com.google.allenday.genomics.core.transform.fn.MergeFn;
import com.google.allenday.genomics.core.transform.fn.SortFn;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;

public class AlignSortMergeTransform extends PTransform<PCollection<KV<GeneExampleMetaData, Iterable<GeneData>>>,
        PCollection<KV<GeneReadGroupMetaData, GeneData>>> {

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
    public PCollection<KV<GeneReadGroupMetaData, GeneData>> expand(PCollection<KV<GeneExampleMetaData, Iterable<GeneData>>> input) {
        return input
                .apply("IterToList transform 1", new ValueIterableToValueListTransform<>())
                .apply(ParDo.of(alignFn))
                .apply(ParDo.of(sortFn))
                .apply(MapElements.via(new SimpleFunction<KV<GeneExampleMetaData, GeneData>, KV<KV<GeneReadGroupMetaData, String>, GeneData>>() {
                    @Override
                    public KV<KV<GeneReadGroupMetaData, String>, GeneData> apply(KV<GeneExampleMetaData, GeneData> input) {
                        GeneExampleMetaData geneExampleMetaData = input.getKey();
                        GeneData geneData = input.getValue();
                        return KV.of(KV.of(geneExampleMetaData, geneData.getReferenceName()), geneData);
                    }
                }))
                .apply(GroupByKey.create())
                .apply("IterToList transform 2", new ValueIterableToValueListTransform<>())
                .apply(ParDo.of(mergeFn));
    }
}
