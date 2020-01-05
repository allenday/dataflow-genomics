package com.google.allenday.genomics.core.processing.vcf_to_bq;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class PrepareVcfToBqBatchFn extends DoFn<KV<BlobId, String>, KV<ReferenceDatabase, String>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareVcfToBqBatchFn.class);
    private final static int BATCH_SIZE = 50;

    private GCSService gcsService;

    private ReferencesProvider referencesProvider;
    private FileUtils fileUtils;
    private String workingBucket;
    private String workingDir;
    private String jobTime;

    public PrepareVcfToBqBatchFn(ReferencesProvider referencesProvider, FileUtils fileUtils, String workingBucket, String workingDir, String jobTime) {
        this.referencesProvider = referencesProvider;
        this.fileUtils = fileUtils;
        this.workingBucket = workingBucket;
        this.workingDir = workingDir;
        this.jobTime = jobTime;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        BlobId vcfPathBlobId = c.element().getKey();
        String referenceName = c.element().getValue();

        List<BlobId> blobsToProcess = gcsService.getAllBlobsIn(vcfPathBlobId.getBucket(),
                vcfPathBlobId.getName())
                .stream()
                .map(BlobInfo::getBlobId)
                .collect(Collectors.toList());

        LOG.info(String.format("Blobs in %s: %d", vcfPathBlobId.toString(), blobsToProcess.size()));
        try {
            ReferenceDatabase referenceDd = referencesProvider.getReferenceDd(gcsService, referenceName);
            for (int batchStart = 0; batchStart < blobsToProcess.size(); batchStart = batchStart + BATCH_SIZE) {
                int limit = (batchStart + BATCH_SIZE) <= blobsToProcess.size() ? (batchStart + BATCH_SIZE) : blobsToProcess.size();
                List<BlobId> batchList = blobsToProcess.subList(batchStart, limit);
                LOG.info("Batch size: "+batchList.size());
                BlobId destDir = BlobId.of(workingBucket, String.format(workingDir + "/temp_%s_%s_%d_%d/",
                        referenceName, jobTime, batchStart, batchStart + BATCH_SIZE - 1));
                batchList.forEach(blobId -> gcsService.copy(blobId, BlobId.of(destDir.getBucket(),
                        destDir.getName() + fileUtils.getFilenameFromPath(blobId.getName()))));
                c.output(KV.of(referenceDd, gcsService.getUriFromBlob(destDir) + "*"));
            }

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }
}
