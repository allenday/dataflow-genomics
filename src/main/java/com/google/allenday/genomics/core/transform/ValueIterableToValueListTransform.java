package com.google.allenday.genomics.core.transform;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ValueIterableToValueListTransform<K, V> extends PTransform<PCollection<KV<K, Iterable<V>>>,
        PCollection<KV<K, List<V>>>> {

    @Override
    public PCollection<KV<K, List<V>>> expand(PCollection<KV<K, Iterable<V>>> input) {
        return input
                .apply(MapElements.via(new SimpleFunction<KV<K, Iterable<V>>, KV<K, List<V>>>() {
                    @Override
                    public KV<K, List<V>> apply(KV<K, Iterable<V>> input) {
                        return KV.of(input.getKey(), StreamSupport.stream(input.getValue().spliterator(), false)
                                .collect(Collectors.toList()));
                    }
                }));
    }
}
