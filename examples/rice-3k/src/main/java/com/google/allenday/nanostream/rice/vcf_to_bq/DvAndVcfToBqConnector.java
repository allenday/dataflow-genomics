package com.google.allenday.nanostream.rice.vcf_to_bq;

import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.model.SraSampleIdReferencePair;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class DvAndVcfToBqConnector extends SimpleFunction<KV<SraSampleIdReferencePair, String>, KV<ReferenceDatabase, String>> {
    @Override
    public KV<ReferenceDatabase, String> apply(KV<SraSampleIdReferencePair, String> input) {
        return KV.of(input.getKey().getReferenceDatabase(), input.getValue());
    }
}