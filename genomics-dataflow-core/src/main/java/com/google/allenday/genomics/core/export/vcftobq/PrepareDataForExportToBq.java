package com.google.allenday.genomics.core.export.vcftobq;

import com.google.allenday.genomics.core.model.SamRecordsChunkMetadataKey;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PrepareDataForExportToBq extends PTransform<PCollection<KV<SamRecordsChunkMetadataKey, KV<String, String>>>, PCollection<KV<String, String>>> {

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
                                        .map(dir -> KV.of(kv.getKey(), dir + "*")).collect(Collectors.toList())));
    }
}
