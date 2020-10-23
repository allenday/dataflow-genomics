package com.google.allenday.genomics.core.pipeline.batch.partsprocessing;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class PrepareAlignNotProcessedFn extends DoFn<KV<SampleRunMetaData, List<FileWrapper>>,
        KV<SampleRunMetaData, KV<List<ReferenceDatabaseSource>, List<FileWrapper>>>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareAlignNotProcessedFn.class);

    private GcsService gcsService;

    private FileUtils fileUtils;
    private List<String> references;

    private StagingPathsBulder stagingPathsBulder;
    private String allReferencesDirGcsUri;


    public PrepareAlignNotProcessedFn(FileUtils fileUtils,
                                      List<String> references,
                                      StagingPathsBulder stagingPathsBulder,
                                      String allReferencesDirGcsUri) {
        this.fileUtils = fileUtils;
        this.references = references;
        this.stagingPathsBulder = stagingPathsBulder;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
    }

    @Setup
    public void setUp() {
        gcsService = GcsService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SampleRunMetaData, List<FileWrapper>> input = c.element();
        SampleRunMetaData geneSampleRunMetaData = input.getKey();

        for (String ref : references) {

            BlobId blobIdAlign = stagingPathsBulder.buildAlignedBlobId(geneSampleRunMetaData.getRunId(), ref);
            boolean exists = gcsService.isExists(blobIdAlign);
            if (!exists) {
                LOG.info(String.format("Pass to %s: %s", "ALIGN", geneSampleRunMetaData.getRunId()));
                c.output(KV.of(geneSampleRunMetaData,
                        KV.of(Collections.singletonList(new ReferenceDatabaseSource.ByNameAndUriSchema(ref, allReferencesDirGcsUri)), input.getValue())));
            }
        }
    }
}
