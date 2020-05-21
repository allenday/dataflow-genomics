package com.google.allenday.genomics.core.pipeline.io;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;


public class TransformIoHandler implements Serializable {

    private static Logger LOG = LoggerFactory.getLogger(TransformIoHandler.class);
    private final static String DEFAULT_DEST_GCS_DIR = "results/";

    private String resultsBucket;
    private String jobDateTime;
    private String destGcsDir;
    private FileUtils fileUtils;
    private long memoryOutputLimitMb = 0;

    public TransformIoHandler(String resultsBucket, FileUtils fileUtils) {
        this.resultsBucket = resultsBucket;
        this.fileUtils = fileUtils;
        this.destGcsDir = DEFAULT_DEST_GCS_DIR;
    }

    public TransformIoHandler(String resultsBucket, FileUtils fileUtils, String jobDateTime) {
        this(resultsBucket, fileUtils);
        this.jobDateTime = jobDateTime;
        this.destGcsDir = jobDateTime + "/" + DEFAULT_DEST_GCS_DIR;
    }

    public TransformIoHandler withTimestampedDestGcsDir(String dirPattern) {
        this.destGcsDir = String.format(dirPattern, jobDateTime);
        return this;
    }

    public TransformIoHandler withDestGcsDir(String dir) {
        this.destGcsDir = dir;
        return this;
    }

    public void overwriteWithTimestampedDestGcsDir(String dirPattern) {
        this.destGcsDir = String.format(dirPattern, jobDateTime);
    }

    public void overwriteWithDestGcsDir(String dir) {
        this.destGcsDir = dir;
    }

    private String destGcsDirWithSubDir(String subDirName){
        return destGcsDir + subDirName + "/";
    }

    public FileWrapper handleContentOutput(GcsService gcsService, byte[] content, String filename, String subDirName) throws IOException {
        FileWrapper fileWrapper;
        if (content.length > fileUtils.mbToBytes(memoryOutputLimitMb)) {
            fileWrapper = saveContentToGcsOutput(gcsService, content, filename, subDirName);
        } else {
            LOG.info(String.format("Pass %s file as CONTENT data", filename));
            fileWrapper = FileWrapper.fromByteArrayContent(content, filename);
        }
        return fileWrapper;
    }

    public FileWrapper saveContentToGcsOutput(GcsService gcsService, byte[] content, String filename, String subDirName) throws IOException {
        String gcsFilePath = destGcsDirWithSubDir(subDirName) + filename;

        LOG.info(String.format("Export %s content to GCS %s", filename, gcsFilePath));

        Blob blob = gcsService.writeToGcs(resultsBucket, gcsFilePath, Channels.newChannel(new ByteArrayInputStream(content)));
        return FileWrapper.fromBlobUri(gcsService.getUriFromBlob(blob.getBlobId()), filename);
    }

    public FileWrapper handleFileOutput(GcsService gcsService, String filepath, String subDirName) throws IOException {
        FileWrapper fileWrapper;
        if (fileUtils.getFileSizeMegaBytes(filepath) > memoryOutputLimitMb) {
            fileWrapper = saveFileToGcsOutput(gcsService, filepath, subDirName);
        } else {
            String fileName = fileUtils.getFilenameFromPath(filepath);
            LOG.info(String.format("Pass %s file as CONTENT data", filepath));
            fileWrapper = FileWrapper.fromByteArrayContent(fileUtils.readFileToByteArray(filepath), fileName);
        }
        fileUtils.deleteFile(filepath);
        return fileWrapper;
    }

    public FileWrapper saveFileToGcsOutput(GcsService gcsService, String filepath, String gcsDestFilename, String subDirName) throws IOException {
        String gcsFilePath = destGcsDirWithSubDir(subDirName) + gcsDestFilename;

        LOG.info(String.format("Export %s file to GCS %s", filepath, gcsFilePath));
        Blob blob = gcsService.writeToGcs(resultsBucket, gcsFilePath, filepath);
        return FileWrapper.fromBlobUri(gcsService.getUriFromBlob(blob.getBlobId()), gcsDestFilename);
    }

    public FileWrapper saveFileToGcsOutput(GcsService gcsService, String filepath, String subDirName) throws IOException {
        String fileName = fileUtils.getFilenameFromPath(filepath);
        return saveFileToGcsOutput(gcsService, filepath, fileName, subDirName);
    }

    public String handleInputAsLocalFile(GcsService gcsService, FileWrapper fileWrapper, String workDir) throws IOException {
        String destFilepath = workDir + fileWrapper.getFileName();
        if (fileWrapper.getDataType() == FileWrapper.DataType.CONTENT) {
            fileUtils.saveDataToFile(fileWrapper.getContent(), destFilepath);
        } else if (fileWrapper.getDataType() == FileWrapper.DataType.BLOB_URI) {
            BlobId blobId = gcsService.getBlobIdFromUri(fileWrapper.getBlobUri());
            gcsService.downloadBlobTo(gcsService.getBlob(blobId), destFilepath);
        }
        return destFilepath;
    }

    public byte[] handleInputAsContent(GcsService gcsService, FileWrapper fileWrapper) {
        if (fileWrapper.getDataType() == FileWrapper.DataType.CONTENT) {
            return fileWrapper.getContent();
        } else if (fileWrapper.getDataType() == FileWrapper.DataType.BLOB_URI) {
            BlobId blobId = gcsService.getBlobIdFromUri(fileWrapper.getBlobUri());
            try {
                return gcsService.readBlob(blobId).getBytes();
            } catch (IOException e) {
                LOG.error(e.getMessage());
                return new byte[0];
            }
        }
        return new byte[0];
    }

    public FileWrapper handleInputAndCopyToGcs(FileWrapper fileWrapper, GcsService gcsService, String newFileName,
                                               String workDir, String subDirName) throws IOException {
        String gcsFilePath = destGcsDirWithSubDir(subDirName) + newFileName;
        Blob resultBlob;
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
        return FileWrapper.fromBlobUri(gcsService.getUriFromBlob(resultBlob.getBlobId()), newFileName);
    }

    public static boolean tryToFindInPrevious(GcsService gcsService,
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

    public void setMemoryOutputLimitMb(long memoryOutputLimitMb) {
        this.memoryOutputLimitMb = memoryOutputLimitMb;
    }
}
