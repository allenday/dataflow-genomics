package com.google.allenday.genomics.core.export.vcftobq;

import com.google.allenday.genomics.core.model.SamRecordsChunkMetadataKey;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Apache Beam PTransform that groups Variant Calling results (VCF) of contig regions and exports them into the <a href="https://cloud.google.com/bigquery">BigQuery</a> table.
 * Uses vcf-to-bigquery transform from <a href="https://github.com/googlegenomics/gcp-variant-transforms">GCP Variant Transforms</a>
 */
public class PrepareAndExecuteVcfToBqTransform extends PTransform<PCollection<KV<SamRecordsChunkMetadataKey, KV<String, String>>>, PCollection<KV<String, String>>> {

    private final static String VCF_EXT = ".vcf";

    private VcfToBqFn vcfToBqFn;

    public PrepareAndExecuteVcfToBqTransform(VcfToBqFn vcfToBqFn) {
        this.vcfToBqFn = vcfToBqFn;
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<KV<SamRecordsChunkMetadataKey, KV<String, String>>> input) {
        return input
                .apply("Prepare for group by VCF output dir", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()), TypeDescriptors.strings()))
                        .via(kv -> KV.of(KV.of(kv.getValue().getValue(), kv.getKey().getReferenceName()), kv.getValue().getKey())))
                .apply("Group by VCF output dir", GroupByKey.create())
                .apply("Prepare for group by reference", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via(kv -> KV.of(kv.getKey().getValue(), kv.getKey().getKey())))
                .apply("Group by reference", GroupByKey.create())
                .apply("Build data entity for Vcf for Bq Fn", FlatMapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via(kv ->
                                StreamSupport.stream(kv.getValue().spliterator(), false)
                                        .map(dir -> KV.of(kv.getKey(), dir + "*" + VCF_EXT)).collect(Collectors.toList())))
                .apply("Vcf to Bq ", ParDo.of(vcfToBqFn));
    }
}
