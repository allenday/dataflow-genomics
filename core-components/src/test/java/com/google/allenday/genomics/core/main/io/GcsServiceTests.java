package com.google.allenday.genomics.core.main.io;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.IoUtils;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

public class GcsServiceTests {

    @Test
    public void testWriteToGcs() throws IOException {
        FileUtils fileUtilsMock = Mockito.mock(FileUtils.class);
        Storage storageMock = Mockito.mock(Storage.class);

        GCSService gcsService = new GCSService(storageMock, fileUtilsMock);


        String bucketName = "bucketName";
        String blobName = "blobName";
        ReadableByteChannel readableByteChannelMock = Mockito.mock(ReadableByteChannel.class);
        WriteChannel writeChannelMock = Mockito.mock(WriteChannel.class);

        Integer firstCallContentLength = 10;

        Mockito.when(readableByteChannelMock.read(Mockito.any())).thenReturn(firstCallContentLength).thenReturn(0);
        Mockito.when(storageMock.writer(Mockito.any())).thenReturn(writeChannelMock);
        gcsService.writeToGcs(bucketName, blobName, readableByteChannelMock);

        Mockito.verify(storageMock).writer(BlobInfo.newBuilder(BlobId.of(bucketName, blobName)).build());
        Mockito.verify(writeChannelMock, Mockito.times(1)).write(Mockito.any());
        Mockito.verify(readableByteChannelMock).close();
    }

    @Test
    public void testGetBlobIdFromUri() {
        FileUtils fileUtilsMock = Mockito.mock(FileUtils.class);
        Storage storageMock = Mockito.mock(Storage.class);

        GCSService gcsService = new GCSService(storageMock, fileUtilsMock);

        String bucketName = "bucketName";
        String blobName = "blobName";
        String blobUri = String.format("gs://%s/%s", bucketName, blobName);

        BlobId gcsServiceBlobIdFromUri = gcsService.getBlobIdFromUri(blobUri);
        Assert.assertEquals(BlobId.of(bucketName, blobName), gcsServiceBlobIdFromUri);
    }

    @Test
    public void testReadBlob() throws Exception {
        FileUtils fileUtilsMock = Mockito.mock(FileUtils.class);
        Storage storageMock = Mockito.mock(Storage.class);
        IoUtils ioUtilsMock = Mockito.mock(IoUtils.class);

        GCSService gcsService = new GCSService(storageMock, fileUtilsMock);

        String bucketName = "bucketName";
        String blobName = "blobName";
        String content = "content";
        Integer firstCallContentLength = 10;

        Blob blobMock = Mockito.mock(Blob.class);
        ReadChannel readChannelMock = Mockito.mock(ReadChannel.class);

        Mockito.when(storageMock.get(Mockito.eq(BlobId.of(bucketName, blobName)))).thenReturn(blobMock);
        Mockito.when(blobMock.reader()).thenReturn(readChannelMock);
        Mockito.when(readChannelMock.read(Mockito.any())).thenReturn(firstCallContentLength).thenReturn(0);
        Mockito.when(ioUtilsMock.getStringContentFromByteBuffer(Mockito.any())).thenReturn(content);
        String result = gcsService.readBlob(ioUtilsMock, bucketName, blobName);

        Mockito.verify(ioUtilsMock, Mockito.times(1)).getStringContentFromByteBuffer(Mockito.any());
        Assert.assertEquals(content, result);
    }
}
