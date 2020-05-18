package com.google.allenday.genomics.core.pipeline.transform;

import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BreakFusion<K, V> extends PTransform<PCollection<KV<K, V>>,
        PCollection<KV<K, V>>> {

    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
        return input.apply(GroupByKey.create())
                .apply(FlatMapElements.via(new InferableFunction<KV<K, Iterable<V>>, Iterable<KV<K, V>>>() {
                    @Override
                    public Iterable<KV<K, V>> apply(KV<K, Iterable<V>> input) throws Exception {
                        return StreamSupport.stream(input.getValue().spliterator(), false)
                                .map(value -> KV.of(input.getKey(), value)).collect(Collectors.toList());
                    }
                }));
    }

    public static <K, V> BreakFusion<K, V> create() {
        return new BreakFusion<K, V>();
    }
}
