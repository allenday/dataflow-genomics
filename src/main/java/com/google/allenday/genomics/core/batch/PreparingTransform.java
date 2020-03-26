package com.google.allenday.genomics.core.batch;

import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.util.List;

public abstract class PreparingTransform extends PTransform<PCollection<KV<SampleMetaData, List<FileWrapper>>>,
        PCollection<KV<SampleMetaData, List<FileWrapper>>>> {

    public PreparingTransform() {
    }

    public PreparingTransform(@Nullable String name) {
        super(name);
    }
}
