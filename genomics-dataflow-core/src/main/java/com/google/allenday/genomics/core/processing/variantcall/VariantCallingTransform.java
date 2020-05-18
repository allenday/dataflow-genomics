package com.google.allenday.genomics.core.processing.variantcall;

import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.SamRecordsChunkMetadataKey;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.allenday.genomics.core.pipeline.transform.BreakFusion;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Apache Beam PTransform,
 * that provides <a href="https://www.ebi.ac.uk/training/online/course/human-genetic-variation-i-introduction-2019/variant-identification-and-analysis">Variant Calling</a> logic.
 */
public class VariantCallingTransform extends PTransform<PCollection<KV<SamRecordsChunkMetadataKey, KV<ReferenceDatabaseSource, BamWithIndexUris>>>, PCollection<KV<SamRecordsChunkMetadataKey, KV<String, String>>>> {

    private VariantCallingFn variantCallingFn;

    public VariantCallingTransform(VariantCallingFn variantCallingFn) {
        this.variantCallingFn = variantCallingFn;
    }

    @Override
    public PCollection<KV<SamRecordsChunkMetadataKey, KV<String, String>>> expand(PCollection<KV<SamRecordsChunkMetadataKey, KV<ReferenceDatabaseSource, BamWithIndexUris>>> input) {
        return input
                .apply(Filter.by(kv -> kv.getKey().getRegion().isMapped()))
                .apply(BreakFusion.create())
                .apply("Variant Calling Fn", ParDo.of(variantCallingFn));
    }
}
