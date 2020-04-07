package com.google.allenday.genomics.core.main.io;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.cloud.storage.Blob;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class TransformIoHandlerTests {

    private final static String RESULT_BUCKET = "resultsBucket";
    private final static String DEST_GCS_PREFIX = "destGcsPrefix";
    private final static String WORK_DIR = "workDir";
    private final static String FILE_NAME = "fileName";
    private final static String BLOB_URI = "blobUri";

    @Test
    public void testHandleInputAsLocalFileResult() {
        FileUtils fileUtilsMock = Mockito.mock(FileUtils.class, Mockito.withSettings().serializable());
        GCSService gcsServiceMock = Mockito.mock(GCSService.class, Mockito.withSettings().serializable());
        FileWrapper fileWrapperMock = Mockito.mock(FileWrapper.class, Mockito.withSettings().serializable());

        Mockito.when(fileWrapperMock.getFileName()).thenReturn(FILE_NAME);

        TransformIoHandler transformIoHandler = new TransformIoHandler(RESULT_BUCKET, fileUtilsMock)
                .withDestGcsDir(DEST_GCS_PREFIX);
        String result = transformIoHandler.handleInputAsLocalFile(gcsServiceMock, fileWrapperMock, WORK_DIR);

        Assert.assertEquals("Result asserting", WORK_DIR + FILE_NAME, result);
    }

    @Test
    public void testHandleInputAsLocalFileBlobUriBranch() {
        FileUtils fileUtilsMock = Mockito.mock(FileUtils.class, Mockito.withSettings().serializable());
        GCSService gcsServiceMock = Mockito.mock(GCSService.class, Mockito.withSettings().serializable());
        FileWrapper fileWrapperMock = Mockito.mock(FileWrapper.class, Mockito.withSettings().serializable());
        Blob blobMock = Mockito.mock(Blob.class, Mockito.withSettings().serializable());

        Mockito.when(fileWrapperMock.getFileName()).thenReturn(FILE_NAME);
        Mockito.when(fileWrapperMock.getDataType()).thenReturn(FileWrapper.DataType.BLOB_URI);
        Mockito.when(fileWrapperMock.getBlobUri()).thenReturn(BLOB_URI);
        Mockito.when(gcsServiceMock.getBlob(Mockito.any())).thenReturn(blobMock);

        TransformIoHandler transformIoHandler = new TransformIoHandler(RESULT_BUCKET, fileUtilsMock)
                .withDestGcsDir(DEST_GCS_PREFIX);
        transformIoHandler.handleInputAsLocalFile(gcsServiceMock, fileWrapperMock, WORK_DIR);

        Mockito.verify(gcsServiceMock).getBlobIdFromUri(BLOB_URI);
        Mockito.verify(gcsServiceMock).downloadBlobTo(blobMock, WORK_DIR + FILE_NAME);
    }

    @Test
    public void testHandleInputAsLocalFileContentBranch() throws IOException {
        FileUtils fileUtilsMock = Mockito.mock(FileUtils.class, Mockito.withSettings().serializable());
        GCSService gcsServiceMock = Mockito.mock(GCSService.class, Mockito.withSettings().serializable());
        FileWrapper fileWrapperMock = Mockito.mock(FileWrapper.class, Mockito.withSettings().serializable());

        byte[] byteArray = "content".getBytes();

        Mockito.when(fileWrapperMock.getFileName()).thenReturn(FILE_NAME);
        Mockito.when(fileWrapperMock.getDataType()).thenReturn(FileWrapper.DataType.CONTENT);
        Mockito.when(fileWrapperMock.getContent()).thenReturn(byteArray);

        TransformIoHandler transformIoHandler = new TransformIoHandler(RESULT_BUCKET, fileUtilsMock)
                .withDestGcsDir(DEST_GCS_PREFIX);
        transformIoHandler.handleInputAsLocalFile(gcsServiceMock, fileWrapperMock, WORK_DIR);

        Mockito.verify(fileUtilsMock).saveDataToFile(byteArray, WORK_DIR + FILE_NAME);
    }
}
