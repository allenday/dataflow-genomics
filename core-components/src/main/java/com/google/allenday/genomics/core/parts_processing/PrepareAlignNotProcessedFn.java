package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class PrepareAlignNotProcessedFn extends DoFn<KV<SampleMetaData, List<FileWrapper>>,
        KV<KV<SampleMetaData, List<String>>, List<FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareAlignNotProcessedFn.class);

    private GCSService gcsService;

    private FileUtils fileUtils;
    private List<String> references;

    private StagingPathsBulder stagingPathsBulder;


    public PrepareAlignNotProcessedFn(FileUtils fileUtils, List<String> references, StagingPathsBulder stagingPathsBulder) {
        this.fileUtils = fileUtils;
        this.references = references;
        this.stagingPathsBulder = stagingPathsBulder;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SampleMetaData, List<FileWrapper>> input = c.element();
        SampleMetaData geneSampleMetaData = input.getKey();

        for (String ref : references) {

            BlobId blobIdAlign = stagingPathsBulder.buildAlignedBlobId(geneSampleMetaData.getRunId(), ref);
            boolean exists = gcsService.isExists(blobIdAlign);
            if (!exists) {
                LOG.info(String.format("Pass to %s: %s", "ALIGN", geneSampleMetaData.getRunId()));
                c.output(KV.of(KV.of(geneSampleMetaData, Collections.singletonList(ref)), input.getValue()));
            }
        }
    }
}
