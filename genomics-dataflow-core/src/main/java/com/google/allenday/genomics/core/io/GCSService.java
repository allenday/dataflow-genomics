package com.google.allenday.genomics.core.io;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Provides access to {@link Storage} instance with convenient interface
 */
public class GCSService implements Serializable {

    private final static int DEFAULT_BLOB_MAX_PAGE_SIZE_TO_RETRIVE = 100000;

    private Logger LOG = LoggerFactory.getLogger(GCSService.class);

    private Storage storage;
    private FileUtils fileUtils;

    public GCSService(Storage storage, FileUtils fileUtils) {
        this.storage = storage;
        this.fileUtils = fileUtils;
    }

    public static GCSService initialize(FileUtils fileUtils) {
        return new GCSService(StorageOptions.getDefaultInstance().getService(), fileUtils);
    }

    public Blob getBlob(BlobId blobId) throws StorageException {
        return storage.get(blobId);
    }

    public Blob getBlob(String bucketName, String blobName) throws StorageException {
        return getBlob(BlobId.of(bucketName, blobName));
    }

    public Blob saveContentToGcs(BlobId blobId, byte[] content) {
        return storage.create(BlobInfo.newBuilder(blobId).build(), content);
    }

    public Blob writeToGcs(String bucketName, String blobName, String filePath) throws IOException {
        RandomAccessFile srcFile = new RandomAccessFile(filePath, "r");
        FileChannel inChannel = srcFile.getChannel();

        return writeToGcs(bucketName, blobName, inChannel);
    }

    public Blob writeToGcs(String bucketName, String blobName, ReadableByteChannel inChannel) throws IOException {
        LOG.info(String.format("Uploading data to %s", getUriFromBlob(BlobId.of(bucketName, blobName))));
        BlobId blobId = BlobId.of(bucketName, blobName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);

        try (WriteChannel writer = storage.writer(blobInfo)) {
            while (inChannel.read(buffer) > 0) {
                buffer.flip();
                try {
                    writer.write(buffer);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                buffer.clear();
            }
        }
        inChannel.close();
        LOG.info(String.format("Data uploaded to %s", getUriFromBlob(BlobId.of(bucketName, blobName))));
        return getBlob(bucketName, blobName);
    }


    public String getUriFromBlob(BlobId blobId) {
        return String.format("gs://%s/%s", blobId.getBucket(), blobId.getName());
    }

    public Blob copy(BlobId srcBlobId, BlobId destBlobId) {
        CopyWriter copyWriter = storage.copy(Storage.CopyRequest.newBuilder()
                .setSource(srcBlobId)
                .setTarget(destBlobId)
                .build());
        return copyWriter.getResult();
    }

    public BlobId getBlobIdFromUri(String uri) {
        try {
            String workPart = uri.split("//")[1];
            String[] parts = workPart.split("/");
            String bucket = parts[0];
            String name = workPart.replace(bucket + "/", "");
            return BlobId.of(bucket, name);
        } catch (Exception e) {
            return null;
        }
    }

    public ReadChannel getBlobReadChannel(BlobId blobId) throws StorageException {
        return storage.reader(blobId);
    }


    public Iterable<Blob> getBlobsWithPrefix(String bucketName, String prefix) throws StorageException {
        return storage.list(bucketName, Storage.BlobListOption.prefix(prefix),
                Storage.BlobListOption.pageSize(DEFAULT_BLOB_MAX_PAGE_SIZE_TO_RETRIVE)).iterateAll();
    }

    public List<BlobId> getBlobsIdsWithPrefixList(String bucketName, String prefix) throws StorageException {
        return StreamSupport.stream(getBlobsWithPrefix(bucketName, prefix).spliterator(), false).map(BlobInfo::getBlobId)
                .collect(Collectors.toList());
    }

    public Blob composeBlobs(Iterable<BlobId> blobIds, BlobId headers, BlobId destBlob) throws StorageException {
        Storage.ComposeRequest composeRequest = Storage.ComposeRequest
                .newBuilder()
                .addSource(headers.getName())
                .addSource(StreamSupport.stream(blobIds.spliterator(), false)
                        .map(BlobId::getName).collect(Collectors.toList()))
                .setTarget(BlobInfo.newBuilder(destBlob).build())
                .build();
        return storage.compose(composeRequest);
    }


    public boolean isExists(BlobId blobId) {
        return Optional.ofNullable(storage.get(blobId)).map(Blob::exists).orElse(false);
    }


    public void downloadBlobTo(Blob blob, String filePath) {
        LOG.info(String.format("Start downloading blob gs://%s/%s with size %d into %s", blob.getBucket(), blob.getName(), blob.getSize(), filePath));
        blob.downloadTo(Paths.get(filePath));
        LOG.info(String.format("Blob gs://%s/%s successfully downloaded into %s", blob.getBucket(), blob.getName(), filePath));
        LOG.info(String.format("Free disk space: %d", fileUtils.getFreeDiskSpace()));
    }

    public String readBlob(BlobId blobId, IoUtils ioUtils) throws IOException, NullPointerException {
        ReadChannel reader = getBlobReadChannel(blobId);

        ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
        StringBuilder builder = new StringBuilder();
        while (reader.read(bytes) > 0) {
            bytes.flip();
            builder.append(ioUtils.getStringContentFromByteBuffer(bytes));
            bytes.clear();
        }
        reader.close();
        return builder.toString();
    }

    public long getBlobSize(BlobId blobId) {
        return storage.get(blobId).getSize();
    }

    public boolean deleteBlobFromGcs(BlobId blobId) {
        return storage.delete(blobId);
    }
}
