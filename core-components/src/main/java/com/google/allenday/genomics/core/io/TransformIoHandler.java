package com.google.allenday.genomics.core.io;

import com.google.allenday.genomics.core.model.FileWrapper;
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

    public FileWrapper handleFileOutput(GCSService gcsService, String filepath) throws IOException {
        FileWrapper fileWrapper;
        if (fileUtils.getFileSizeMegaBytes(filepath) > memoryOutputLimitMb) {
            fileWrapper = saveFileToGcsOutput(gcsService, filepath);
        } else {
            String fileName = fileUtils.getFilenameFromPath(filepath);
            LOG.info(String.format("Pass %s file as CONTENT data", filepath));
            fileWrapper = FileWrapper.fromByteArrayContent(fileUtils.readFileToByteArray(filepath), fileName);
        }
        fileUtils.deleteFile(filepath);
        return fileWrapper;
    }

    public FileWrapper saveFileToGcsOutput(GCSService gcsService, String filepath, String gcsDestFilename) throws IOException {
        String gcsFilePath = destGcsPrefix + gcsDestFilename;

        LOG.info(String.format("Export %s file to GCS %s", filepath, gcsFilePath));
        Blob blob = gcsService.writeToGcs(resultsBucket, gcsFilePath, filepath);
        return FileWrapper.fromBlobUri(gcsService.getUriFromBlob(blob.getBlobId()), gcsDestFilename);
    }

    public FileWrapper saveFileToGcsOutput(GCSService gcsService, String filepath) throws IOException {
        String fileName = fileUtils.getFilenameFromPath(filepath);
        return saveFileToGcsOutput(gcsService, filepath, fileName);
    }

    public String handleInputAsLocalFile(GCSService gcsService, FileWrapper fileWrapper, String workDir) {
        String destFilepath = workDir + fileWrapper.getFileName();
        if (fileWrapper.getDataType() == FileWrapper.DataType.CONTENT) {
            try {
                fileUtils.saveDataToFile(fileWrapper.getContent(), destFilepath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (fileWrapper.getDataType() == FileWrapper.DataType.BLOB_URI) {
            BlobId blobId = gcsService.getBlobIdFromUri(fileWrapper.getBlobUri());
            gcsService.downloadBlobTo(gcsService.getBlob(blobId), destFilepath);
        }
        return destFilepath;
    }

    public FileWrapper handleInputAndCopyToGcs(FileWrapper fileWrapper, GCSService gcsService, String newFileName, String workDir) throws RuntimeException {
        String gcsFilePath = destGcsPrefix + newFileName;
        Blob resultBlob;
        try {
            if (fileWrapper.getDataType() == FileWrapper.DataType.CONTENT) {
                String filePath = workDir + newFileName;
                fileUtils.saveDataToFile(fileWrapper.getContent(), filePath);

                resultBlob = gcsService.writeToGcs(resultsBucket, gcsFilePath, filePath);
            } else if (fileWrapper.getDataType() == FileWrapper.DataType.BLOB_URI) {
                resultBlob = gcsService.copy(gcsService.getBlobIdFromUri(fileWrapper.getBlobUri()),
                        BlobId.of(resultsBucket, gcsFilePath));
            } else {
                throw new RuntimeException("Gene data type should be CONTENT or BLOB_URI");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return FileWrapper.fromBlobUri(gcsService.getUriFromBlob(resultBlob.getBlobId()), newFileName);
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
