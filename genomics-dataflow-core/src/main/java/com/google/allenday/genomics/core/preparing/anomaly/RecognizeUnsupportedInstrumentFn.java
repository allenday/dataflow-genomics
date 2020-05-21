package com.google.allenday.genomics.core.preparing.anomaly;

import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.runfile.FastqInputResource;
import com.google.allenday.genomics.core.processing.align.Instrument;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RecognizeUnsupportedInstrumentFn extends DoFn<KV<SampleRunMetaData, List<FastqInputResource>>,
        KV<SampleRunMetaData, List<FastqInputResource>>> {

    private Logger LOG = LoggerFactory.getLogger(RecognizeUnsupportedInstrumentFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SampleRunMetaData, List<FastqInputResource>> input = c.element();
        LOG.info(String.format("RecognizeUnsupportedInstrumentFn %s", input.toString()));

        SampleRunMetaData geneSampleMetaData = input.getKey();
        List<FastqInputResource> originalGeneDataList = input.getValue();

        if (geneSampleMetaData == null || originalGeneDataList == null) {
            LOG.error("Data error {}, {}", geneSampleMetaData, originalGeneDataList);
            return;
        }

        if (Arrays.stream(Instrument.values()).map(Enum::name).noneMatch(instrumentName -> instrumentName.equals(geneSampleMetaData.getPlatform()))) {
            geneSampleMetaData.setComment(String.format("Unknown INSTRUMENT: %s", geneSampleMetaData.getPlatform()));
            c.output(KV.of(geneSampleMetaData, Collections.emptyList()));
        } else {
            c.output(c.element());
        }
    }
}
