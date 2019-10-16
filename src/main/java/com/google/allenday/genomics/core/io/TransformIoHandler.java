package com.google.allenday.genomics.core.io;

import com.google.allenday.genomics.core.gene.GeneData;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;


public class TransformIoHandler implements Serializable {

    private static Logger LOG = LoggerFactory.getLogger(TransformIoHandler.class);

    private String resultsBucket;
    private String destGcsPrefix;
    private long memoryOutputLimitMb;
    private FileUtils fileUtils;

    public TransformIoHandler(String resultsBucket, String destGcsPrefix, long memoryOutputLimitMb, FileUtils fileUtils) {
        this.resultsBucket = resultsBucket;
        this.destGcsPrefix = destGcsPrefix;
        this.memoryOutputLimitMb = memoryOutputLimitMb;
        this.fileUtils = fileUtils;
    }

    public GeneData handleFileOutput(GCSService gcsService, String filepath, String referenceName) throws IOException {
        GeneData geneData;
        if (fileUtils.getFileSizeMegaBytes(filepath) > memoryOutputLimitMb) {
            geneData = saveFileToGcsOutput(gcsService, filepath, referenceName);
        } else {
            String fileName = fileUtils.getFilenameFromPath(filepath);
            LOG.info(String.format("Pass %s file as CONTENT data", filepath));
            geneData = GeneData.fromByteArrayContent(fileUtils.readFileToByteArray(filepath), fileName).withReferenceName(referenceName);
        }
        fileUtils.deleteFile(filepath);
        return geneData;
    }

    public GeneData saveFileToGcsOutput(GCSService gcsService, String filepath, String referenceName) throws IOException {
        String fileName = fileUtils.getFilenameFromPath(filepath);
        String gcsFilePath = destGcsPrefix + fileName;

        LOG.info(String.format("Export %s file to GCS %s", filepath, gcsFilePath));
        Blob blob = gcsService.writeToGcs(resultsBucket, gcsFilePath, filepath);
        return GeneData.fromBlobUri(gcsService.getUriFromBlob(blob.getBlobId()), fileName).withReferenceName(referenceName);
    }

    public String handleInputAsLocalFile(GCSService gcsService, GeneData geneData, String workDir) {
        String destFilepath = workDir + geneData.getFileName();
        if (geneData.getDataType() == GeneData.DataType.CONTENT) {
            try {
                fileUtils.saveDataToFile(geneData.getContent(), destFilepath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (geneData.getDataType() == GeneData.DataType.BLOB_URI) {
            BlobId blobId = gcsService.getBlobIdFromUri(geneData.getBlobUri());
            gcsService.downloadBlobTo(gcsService.getBlob(blobId), destFilepath);
        }
        return destFilepath;
    }

    public GeneData handleInputAndCopyToGcs(GeneData geneData, GCSService gcsService, String newFileName, String reference, String workDir) throws RuntimeException {
        String gcsFilePath = destGcsPrefix + newFileName;
        Blob resultBlob;
        try {
            if (geneData.getDataType() == GeneData.DataType.CONTENT) {
                String filePath = workDir + newFileName;
                fileUtils.saveDataToFile(geneData.getContent(), filePath);

                resultBlob = gcsService.writeToGcs(resultsBucket, gcsFilePath, filePath);
            } else if (geneData.getDataType() == GeneData.DataType.BLOB_URI) {
                resultBlob = gcsService.copy(gcsService.getBlobIdFromUri(geneData.getBlobUri()),
                        BlobId.of(resultsBucket, gcsFilePath));
            } else {
                throw new RuntimeException("Gene data type should be CONTENT or BLOB_URI");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return GeneData.fromBlobUri(gcsService.getUriFromBlob(resultBlob.getBlobId()), newFileName).withReferenceName(reference);
    }

    public static boolean tryToFindInPrevious(GCSService gcsService,
                                              String alignedSamName,
                                              String alignedSamPath,
                                              String previouBucket,
                                              String previousDestGcsPrefix) {
        BlobId previousBlobId = BlobId.of(previouBucket, previousDestGcsPrefix + alignedSamName);
        LOG.info(String.format("Trying to find %s", previousBlobId.toString()));
        if (gcsService.isExists(previousBlobId)) {
            LOG.info(String.format("File %s found in previous run bucket", alignedSamName));
            gcsService.downloadBlobTo(gcsService.getBlob(previousBlobId), alignedSamPath);
            return true;
        } else {
            return false;
        }
    }
}
