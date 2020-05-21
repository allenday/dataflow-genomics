package com.google.allenday.genomics.core.preparing.anomaly;

import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.custom.FastqInputResourcePreparingTransform;
import com.google.allenday.genomics.core.preparing.runfile.FastqInputResource;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.List;
import java.util.Optional;

public class DetectMissedFilesAndUnsupportedInstrumentTransform extends FastqInputResourcePreparingTransform {

    private String resultBucket;
    private String anomalyOutputPath;
    private RecognizeUnsupportedInstrumentFn recognizeUnsupportedInstrumentFn;
    private RecognizeMissedRunFilesFn recognizeMissedRunFilesFn;

    public DetectMissedFilesAndUnsupportedInstrumentTransform(String resultBucket, String anomalyOutputPath,
                                                              RecognizeUnsupportedInstrumentFn recognizeUnsupportedInstrumentFn,
                                                              RecognizeMissedRunFilesFn recognizeMissedRunFilesFn) {
        this.resultBucket = resultBucket;
        this.anomalyOutputPath = anomalyOutputPath;
        this.recognizeUnsupportedInstrumentFn = recognizeUnsupportedInstrumentFn;
        this.recognizeMissedRunFilesFn = recognizeMissedRunFilesFn;
    }

    @Override
    public PCollection<KV<SampleRunMetaData, List<FastqInputResource>>> expand(PCollection<KV<SampleRunMetaData, List<FastqInputResource>>> input) {
        PCollection<KV<SampleRunMetaData, List<FastqInputResource>>> recognizeAnomaly = input
                .apply("Recognize unsupported instrumnet", ParDo.of(recognizeUnsupportedInstrumentFn))
                .apply("Recognize missed run files", ParDo.of(recognizeMissedRunFilesFn));


        MapElements.into(TypeDescriptors.strings()).via((KV<SampleRunMetaData, List<FastqInputResource>> kvInput) ->
                Optional.ofNullable(kvInput.getKey())
                        .map(geneSampleMetaData -> geneSampleMetaData.getComment() + "," + geneSampleMetaData.getRawMetaData())
                        .orElse("")
        );

        recognizeAnomaly
                .apply("Filter anomaly elements", Filter.by(element -> element.getValue().size() == 0))
                .apply(MapElements.into(TypeDescriptors.strings()).via((KV<SampleRunMetaData, List<FastqInputResource>> kvInput) ->
                        Optional.ofNullable(kvInput.getKey())
                                .map(geneSampleMetaData -> geneSampleMetaData.getComment() + "," + geneSampleMetaData.getRawMetaData())
                                .orElse("")
                ))
                .apply(TextIO.write().withNumShards(1).to(String.format("gs://%s/%s", resultBucket, anomalyOutputPath)));

        return recognizeAnomaly.apply("Filter elements without anomalis", Filter.by(element -> element.getValue().size() > 0));
    }
}
