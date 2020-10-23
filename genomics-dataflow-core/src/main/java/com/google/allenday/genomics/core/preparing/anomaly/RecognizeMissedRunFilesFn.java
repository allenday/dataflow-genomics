package com.google.allenday.genomics.core.preparing.anomaly;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.runfile.FastqInputResource;
import com.google.allenday.genomics.core.processing.align.Instrument;
import com.google.allenday.genomics.core.utils.FileUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RecognizeMissedRunFilesFn extends DoFn<KV<SampleRunMetaData, List<FastqInputResource>>,
        KV<SampleRunMetaData, List<FastqInputResource>>> {

    private Logger LOG = LoggerFactory.getLogger(RecognizeMissedRunFilesFn.class);

    private GcsService gcsService;
    private FileUtils fileUtils;

    public RecognizeMissedRunFilesFn(FileUtils fileUtils) {
        this.fileUtils = fileUtils;
    }

    @Setup
    public void setUp() {
        gcsService = GcsService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SampleRunMetaData, List<FastqInputResource>> input = c.element();
        LOG.info(String.format("RecognizePairedReadsWithAnomalyFn %s", input.toString()));

        SampleRunMetaData geneSampleMetaData = input.getKey();
        List<FastqInputResource> originalGeneDataList = input.getValue();

        if (geneSampleMetaData == null || originalGeneDataList == null) {
            LOG.error("Data error {}, {}", geneSampleMetaData, originalGeneDataList);
            return;
        }
        if (originalGeneDataList.stream().anyMatch(fastqInputResource -> !fastqInputResource.exists(gcsService))) {
            geneSampleMetaData.setComment(geneSampleMetaData.getComment() + "Some run files are missed");
            c.output(KV.of(geneSampleMetaData, Collections.emptyList()));
        } else {
            c.output(c.element());
        }
    }
}
