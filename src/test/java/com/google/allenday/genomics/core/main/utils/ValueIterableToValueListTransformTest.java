package com.google.allenday.genomics.core.main.utils;

import com.google.allenday.genomics.core.align.AlignService;
import com.google.allenday.genomics.core.align.transform.AlignFn;
import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import com.google.allenday.genomics.core.utils.ValueIterableToValueListTransform;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;


public class ValueIterableToValueListTransformTest implements Serializable {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void testValueIterableToValueListTransform() throws IOException {

        String key = "key";
        List<String> value = Arrays.asList("1","2","3");

        PCollection<KV<String, List<String>>> alignedData = testPipeline
                .apply(Create.<KV<String, Iterable<String>>>of(KV.of(key, value)))
                .apply(new ValueIterableToValueListTransform<>());

        PAssert.that(alignedData)
                .satisfies(new SimpleFunction<Iterable<KV<String, List<String>>>, Void>() {
                    @Override
                    public Void apply(Iterable<KV<String, List<String>>> input) {
                        List<KV<String, List<String>>> outputList = StreamSupport.stream(input.spliterator(), false).collect(Collectors.toList());

                        Assert.assertEquals("Output list equals to 1", outputList.size(), 1);
                        Assert.assertEquals("Output list contains input key", outputList.get(0).getKey(), key);
                        Assert.assertEquals("Output list contains input value", outputList.get(0).getValue(), value);

                        return null;
                    }

                });
        PipelineResult pipelineResult = testPipeline.run();
        pipelineResult.waitUntilFinish();
    }
}