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

import java.util.List;

public class RemoveFailedMergeBamsFn extends DoFn<KV<SampleMetaData, List<FileWrapper>>, String> {

    private Logger LOG = LoggerFactory.getLogger(RemoveFailedMergeBamsFn.class);

    private GCSService gcsService;

    private FileUtils fileUtils;
    private List<String> references;
    private String stagedBucket;

    private String sortedFilePattern;
    private String mergedFilePattern;

    private boolean onlyLog;


    public RemoveFailedMergeBamsFn(FileUtils fileUtils, List<String> references,
                                   String stagedBucket, String sortedFilePattern, String mergedFilePattern) {
        this(fileUtils, references, stagedBucket, sortedFilePattern, mergedFilePattern, false);
    }

    public RemoveFailedMergeBamsFn(FileUtils fileUtils, List<String> references,
                                   String stagedBucket, String sortedFilePattern, String mergedFilePattern,
                                   boolean onlyLog) {
        this.fileUtils = fileUtils;
        this.references = references;
        this.stagedBucket = stagedBucket;
        this.sortedFilePattern = sortedFilePattern;
        this.mergedFilePattern = mergedFilePattern;
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
            BlobId blobIdSort = BlobId.of(stagedBucket, String.format(sortedFilePattern, geneSampleMetaData.getRunId(), ref));
            BlobId blobIdMerge = BlobId.of(stagedBucket, String.format(mergedFilePattern, geneSampleMetaData.getSraSample(), ref));
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
