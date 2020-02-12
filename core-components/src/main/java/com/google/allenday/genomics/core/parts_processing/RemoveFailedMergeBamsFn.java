package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RemoveFailedMergeBamsFn extends DoFn<KV<SampleMetaData, List<FileWrapper>>, SraSampleId> {

    private Logger LOG = LoggerFactory.getLogger(RemoveFailedMergeBamsFn.class);

    private GCSService gcsService;

    private FileUtils fileUtils;
    private List<String> references;

    private StagingPathsBulder stagingPathsBulder;

    private boolean onlyLog;


    public RemoveFailedMergeBamsFn(FileUtils fileUtils, List<String> references, StagingPathsBulder stagingPathsBulder) {
        this(fileUtils, references, stagingPathsBulder, false);
    }

    public RemoveFailedMergeBamsFn(FileUtils fileUtils, List<String> references, StagingPathsBulder stagingPathsBulder,
                                   boolean onlyLog) {
        this.fileUtils = fileUtils;
        this.references = references;
        this.stagingPathsBulder = stagingPathsBulder;
        this.onlyLog = onlyLog;
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
            BlobId blobIdSort = stagingPathsBulder.buildSortedBlobId(geneSampleMetaData.getRunId(), ref);
            BlobId blobIdMerge = stagingPathsBulder.buildMergedBlobId(geneSampleMetaData.getSraSample().getValue(), ref);
            boolean existsSort = gcsService.isExists(blobIdSort);
            boolean existsMerge = gcsService.isExists(blobIdMerge);

            if (!existsSort && existsMerge) {
                LOG.info("FOUND: " + geneSampleMetaData.getSraSample());
                if (!onlyLog) {
                    gcsService.deleteBlobFromGcs(blobIdMerge);
                }
                c.output(geneSampleMetaData.getSraSample());
            }
        }
    }
}
