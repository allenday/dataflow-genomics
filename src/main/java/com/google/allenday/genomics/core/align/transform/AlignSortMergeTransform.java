package com.google.allenday.genomics.core.align.transform;

import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.gene.GeneReadGroupMetaData;
import com.google.allenday.genomics.core.utils.ValueIterableToValueListTransform;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.util.List;

public class AlignSortMergeTransform extends PTransform<PCollection<KV<GeneExampleMetaData, List<GeneData>>>,
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
    public PCollection<KV<GeneReadGroupMetaData, GeneData>> expand(PCollection<KV<GeneExampleMetaData, List<GeneData>>> input) {
        return input
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
                .apply("IterToList utils 2", new ValueIterableToValueListTransform<>())
                .apply(ParDo.of(mergeFn));
    }
}
