package com.google.allenday.nanostream.rice.anomaly;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RecognizePairedReadsWithAnomalyFn extends DoFn<KV<SampleMetaData, List<FileWrapper>>, KV<SampleMetaData, List<FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(RecognizePairedReadsWithAnomalyFn.class);

    private String srcBucket;
    private GCSService gcsService;
    private FileUtils fileUtils;

    public RecognizePairedReadsWithAnomalyFn(String srcBucket, FileUtils fileUtils) {
        this.srcBucket = srcBucket;
        this.fileUtils = fileUtils;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SampleMetaData, List<FileWrapper>> input = c.element();
        LOG.info(String.format("RecognizePairedReadsWithAnomalyFn %s", input.toString()));

        List<FileWrapper> originalGeneDataList = input.getValue();
        SampleMetaData geneSampleMetaData = input.getKey();
        try {
            List<FileWrapper> checkedGeneDataList = new ArrayList<>();
            if (originalGeneDataList.size() > 0) {
                for (FileWrapper fileWrapper : originalGeneDataList) {
                    boolean exists = gcsService.isExists(gcsService.getBlobIdFromUri(fileWrapper.getBlobUri()));
                    if (!exists) {
                        logAnomaly(new ArrayList<>(), geneSampleMetaData);
                        geneSampleMetaData.setComment("File not found");
                    } else {
                        checkedGeneDataList.add(fileWrapper);
                    }
                }
                if (originalGeneDataList.size() == checkedGeneDataList.size()) {
                    c.output(KV.of(geneSampleMetaData, checkedGeneDataList));
                } else {
                    geneSampleMetaData.setComment(geneSampleMetaData.getComment() + String.format(" (%d/%d)", checkedGeneDataList.size(), originalGeneDataList.size()));
                    c.output(KV.of(geneSampleMetaData, Collections.emptyList()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void logAnomaly(List<Blob> blobs, SampleMetaData geneSampleMetaData) {
        LOG.info(String.format("Anomaly: %s, %s, %s, blobs %s",
                geneSampleMetaData.getCenterName(),
                geneSampleMetaData.getSraSample(),
                geneSampleMetaData.getRunId(),
                blobs.stream().map(BlobInfo::getName).collect(Collectors.joining(", "))));
    }
}
