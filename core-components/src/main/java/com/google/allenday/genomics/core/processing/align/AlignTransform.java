package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.util.List;

public class AlignTransform extends PTransform<PCollection<KV<SampleMetaData, List<FileWrapper>>>,
        PCollection<KV<KV<SampleMetaData, ReferenceDatabase>, FileWrapper>>> {

    private AlignFn alignFn;
    private List<String> referenceNames;

    public AlignTransform(@Nullable String name, AlignFn alignFn, List<String> referenceNames) {
        super(name);
        this.alignFn = alignFn;
        this.referenceNames = referenceNames;
    }

    public PCollection<KV<KV<SampleMetaData, ReferenceDatabase>, FileWrapper>> expand(
            PCollection<KV<SampleMetaData, List<FileWrapper>>> input) {
        return input.apply("Add all references", ParDo.of(new DoFn<KV<SampleMetaData, List<FileWrapper>>,
                KV<KV<SampleMetaData, List<String>>, List<FileWrapper>>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(KV.of(KV.of(c.element().getKey(), referenceNames), c.element().getValue()));

            }
        }))
                .apply("Align reads", ParDo.of(alignFn));
    }
}
