package com.google.allenday.genomics.core.transform;

import com.google.allenday.genomics.core.model.SraSampleIdReferencePair;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Objects;

public class SampleIdRefPairToRefKeyTransform<T> extends PTransform<PCollection<KV<SraSampleIdReferencePair, T>>, PCollection<KV<String, T>>> {

    @Override
    public PCollection<KV<String, T>> expand(PCollection<KV<SraSampleIdReferencePair, T>> input) {
        return input.apply(MapElements.via(new SimpleFunction<KV<SraSampleIdReferencePair, T>, KV<String, T>>() {
            @Override
            public KV<String, T> apply(KV<SraSampleIdReferencePair, T> input) {
                return KV.of(Objects.requireNonNull(input.getKey()).getReferenceName(), input.getValue());
            }
        }));
    }
}
