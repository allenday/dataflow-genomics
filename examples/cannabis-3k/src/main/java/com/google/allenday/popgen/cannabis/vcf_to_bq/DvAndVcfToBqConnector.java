package com.google.allenday.popgen.cannabis.vcf_to_bq;

import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import org.apache.beam.sdk.values.KV;

public class DvAndVcfToBqConnector extends SimpleFunction<KV<KV<ReadGroupMetaData, ReferenceDatabase>, String>, KV<ReferenceDatabase, String>> {
    @Override
    public KV<ReferenceDatabase, String> apply(KV<KV<ReadGroupMetaData, ReferenceDatabase>, String> input) {
        return KV.of(input.getKey().getValue(), input.getValue());
    }
}