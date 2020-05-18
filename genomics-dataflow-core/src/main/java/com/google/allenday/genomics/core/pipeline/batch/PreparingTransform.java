package com.google.allenday.genomics.core.pipeline.batch;

import com.google.allenday.genomics.core.preparing.runfile.FastqInputResource;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.util.List;

public abstract class PreparingTransform extends PTransform<PCollection<KV<SampleRunMetaData, List<FastqInputResource>>>,
        PCollection<KV<SampleRunMetaData, List<FastqInputResource>>>> {

    public PreparingTransform() {
    }

    public PreparingTransform(@Nullable String name) {
        super(name);
    }
}
