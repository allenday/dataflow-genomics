package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.*;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;

public class PrepareIndexNotProcessedFn extends DoFn<KV<SraSampleId, Iterable<SampleMetaData>>, KV<SraSampleIdReferencePair, FileWrapper>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareIndexNotProcessedFn.class);

    private GCSService gcsService;

    private FileUtils fileUtils;
    private List<String> references;
    private String stagedBucket;

    private String mergedFilePattern;
    private String indexFilePattern;

    public PrepareIndexNotProcessedFn(FileUtils fileUtils, List<String> references,
                                      String stagedBucket, String mergedFilePattern, String indexFilePattern) {
        this.fileUtils = fileUtils;
        this.references = references;
        this.stagedBucket = stagedBucket;
        this.mergedFilePattern = mergedFilePattern;
        this.indexFilePattern = indexFilePattern;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SraSampleId, Iterable<SampleMetaData>> input = c.element();

        @Nonnull
        SraSampleId sraSampleId = input.getKey();

        for (String ref : references) {
            BlobId blobIdMerge = BlobId.of(stagedBucket, String.format(mergedFilePattern, sraSampleId.getValue(), ref));
            boolean mergeExists = gcsService.isExists(blobIdMerge);
            BlobId blobIdIndex = BlobId.of(stagedBucket, String.format(indexFilePattern, sraSampleId.getValue(), ref));
            boolean indexExists = gcsService.isExists(blobIdIndex);
            if (mergeExists && !indexExists) {
                String uriFromBlob = gcsService.getUriFromBlob(blobIdMerge);
                c.output(KV.of(new SraSampleIdReferencePair(sraSampleId, ReferenceDatabase.onlyName(ref)),
                        FileWrapper.fromBlobUri(uriFromBlob, new FileUtils().getFilenameFromPath(uriFromBlob))));
            }
        }
    }
}
