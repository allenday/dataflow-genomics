package com.google.allenday.genomics.core.preparing.custom;

import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.runfile.SraInputResource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class PassPreparingTransform extends SraInputResourcePreparingTransform {

    public PCollection<KV<SampleRunMetaData, SraInputResource>> expand(PCollection<KV<SampleRunMetaData, SraInputResource>> input) {
        return input.apply(ParDo.of(new DoFn<KV<SampleRunMetaData, SraInputResource>, KV<SampleRunMetaData, SraInputResource>>() {
            @ProcessElement
            public void processElement(ProcessContext processContext){
                processContext.output(processContext.element());
            }
        }));
    }
}
