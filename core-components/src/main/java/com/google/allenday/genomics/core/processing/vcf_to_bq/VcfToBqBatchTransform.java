package com.google.allenday.genomics.core.processing.vcf_to_bq;

import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class VcfToBqBatchTransform extends PTransform<PCollection<KV<BlobId, String>>,
        PCollection<KV<ReferenceDatabase, String>>> {

    private PrepareVcfToBqBatchFn prepareVcfToBqBatchFn;
    private VcfToBqFn vcfToBqFn;

    public VcfToBqBatchTransform(PrepareVcfToBqBatchFn prepareVcfToBqBatchFn, VcfToBqFn vcfToBqFn) {
        this.prepareVcfToBqBatchFn = prepareVcfToBqBatchFn;
        this.vcfToBqFn = vcfToBqFn;
    }

    @Override
    public PCollection<KV<ReferenceDatabase, String>> expand(
            PCollection<KV<BlobId, String>> input) {
        return input.apply("Prepare for export", ParDo.of(prepareVcfToBqBatchFn))
                .apply("Export VCF files to BQ", ParDo.of(vcfToBqFn));
    }
}