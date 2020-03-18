package com.google.allenday.genomics.core.main.reference;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class ReferenceProviderTests {

    private final static String ALL_REFERENCES_DIR_GCS_URI = "allReferencesDirGcsUri";
    private final static String REFERENCE_NAME = "refName";
    private final static String REFERENCE_EXTENSION = ".fa";

    @Test
    public void testFindReference() {
        FileUtils fileUtilsMock = Mockito.mock(FileUtils.class, Mockito.withSettings().serializable());
        GCSService gcsServiceMock = Mockito.mock(GCSService.class, Mockito.withSettings().serializable());
        Blob blobMock = Mockito.mock(Blob.class, Mockito.withSettings().serializable());

        BlobId blobId = BlobId.of("bucket", "name");

        Mockito.when(gcsServiceMock.getAllBlobsIn(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(Collections.singletonList(blobMock));
        Mockito.when(gcsServiceMock.getBlobIdFromUri(Mockito.anyString()))
                .thenReturn(blobId);
        Mockito.when(fileUtilsMock.exists(Mockito.anyString()))
                .thenReturn(false);
        Mockito.when(fileUtilsMock.getFilenameFromPath(Mockito.anyString()))
                .thenReturn(REFERENCE_NAME);
        Mockito.when(blobMock.getName())
                .thenReturn(REFERENCE_NAME + REFERENCE_EXTENSION);

        /*TODO update*/
        /*ReferenceProvider referencesProvider = new ReferenceProvider(fileUtilsMock);

        referencesProvider.getReferenceDbWithDownload(gcsServiceMock, REFERENCE_NAME);

        Mockito.verify(gcsServiceMock).getAllBlobsIn(Mockito.anyString(), Mockito.anyString());
        Mockito.verify(blobMock, Mockito.times(3)).getName();
        Mockito.verify(gcsServiceMock).downloadBlobTo(Mockito.eq(blobMock), Mockito.anyString());*/
    }

}
