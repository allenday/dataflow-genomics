package com.google.allenday.genomics.core.preparing.custom;

import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.runfile.FastqInputResource;
import com.google.allenday.genomics.core.preparing.runfile.SraInputResource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.util.List;

public abstract class SraInputResourcePreparingTransform extends PTransform<PCollection<KV<SampleRunMetaData, SraInputResource>>,
        PCollection<KV<SampleRunMetaData, SraInputResource>>> {

    public SraInputResourcePreparingTransform() {
    }

    public SraInputResourcePreparingTransform(@Nullable String name) {
        super(name);
    }
}
