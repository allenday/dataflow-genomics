package com.google.allenday.genomics.core.main.reference;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.allenday.genomics.core.reference.ReferenceProvider;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class ReferenceProviderTests {

    private FileUtils fileUtilsMock;
    private GCSService gcsServiceMock;
    private ReferenceDatabaseSource refDBSourceMock;
    private ReferenceProvider referenceProvider;
    private String testFastaUri;
    private String testFaiUri;
    private String testDbName;
    private String testFastaName;
    private String testCurrentPath;
    private String testAllRefsLocalPath;

    @Before
    public void prepareTest() {
        fileUtilsMock = Mockito.mock(FileUtils.class, Mockito.withSettings().serializable());
        gcsServiceMock = Mockito.mock(GCSService.class, Mockito.withSettings().serializable());

        refDBSourceMock = Mockito.mock(ReferenceDatabaseSource.class, Mockito.withSettings().serializable());
        Blob refBlobMock = Mockito.mock(Blob.class, Mockito.withSettings().serializable());
        Blob refIndexBlobMock = Mockito.mock(Blob.class, Mockito.withSettings().serializable());

        testFastaUri = "gs://bucket/ref.fasta";
        testFaiUri = "gs://bucket/ref.fasta.fai";

        testFastaName = "ref.fasta";
        testCurrentPath = "/";
        testAllRefsLocalPath = "reference/";
        testDbName = "testDB";

        BlobId refBlobId = BlobId.of("bucket", "ref.fasta");
        BlobId refIndexBlobId = BlobId.of("bucket", "ref.fasta.fai");

        Mockito.when(refBlobMock.getBlobId()).thenReturn(refBlobId);
        Mockito.when(refIndexBlobMock.getBlobId()).thenReturn(refIndexBlobId);

        Mockito.when(refDBSourceMock.getReferenceBlob(gcsServiceMock, fileUtilsMock))
                .thenReturn(Optional.of(refBlobMock));
        Mockito.when(refDBSourceMock.getReferenceIndexBlob(gcsServiceMock, fileUtilsMock))
                .thenReturn(Optional.of(refIndexBlobMock));
        Mockito.when(refDBSourceMock.getName())
                .thenReturn(testDbName);

        Mockito.when(gcsServiceMock.getUriFromBlob(refBlobId))
                .thenReturn(testFastaUri);
        Mockito.when(gcsServiceMock.getUriFromBlob(refIndexBlobId))
                .thenReturn(testFaiUri);

        Mockito.when(fileUtilsMock.getFilenameFromPath(Mockito.anyString()))
                .thenReturn(testFastaName);
        Mockito.when(fileUtilsMock.exists(Mockito.anyString()))
                .thenReturn(false);
        Mockito.when(fileUtilsMock.getCurrentPath())
                .thenReturn(testCurrentPath);


        referenceProvider = new ReferenceProvider(fileUtilsMock, testAllRefsLocalPath);
    }

    @Test
    public void testReferenceDbRemote() {
        ReferenceDatabase referenceDd = referenceProvider.getReferenceDd(gcsServiceMock, refDBSourceMock);

        Mockito.verify(refDBSourceMock).getReferenceBlob(gcsServiceMock, fileUtilsMock);
        Mockito.verify(gcsServiceMock, Mockito.times(2)).getUriFromBlob(Mockito.any());

        Assert.assertEquals(testDbName, referenceDd.getDbName());
        Assert.assertEquals(testFastaUri, referenceDd.getFastaGcsUri());
        Assert.assertEquals(testFaiUri, referenceDd.getFaiGcsUri());
    }

    @Test
    public void testReferenceDbWithDownload() {
        ReferenceDatabase referenceDd = referenceProvider.getReferenceDbWithDownload(gcsServiceMock, refDBSourceMock);

        Mockito.verify(refDBSourceMock).getReferenceBlob(gcsServiceMock, fileUtilsMock);
        Mockito.verify(gcsServiceMock, Mockito.times(2)).getUriFromBlob(Mockito.any());


        Mockito.verify(fileUtilsMock).mkdirFromUri(Mockito.anyString());
        Mockito.verify(gcsServiceMock).downloadBlobTo(Mockito.any(), Mockito.anyString());

        Assert.assertEquals(testDbName, referenceDd.getDbName());
        Assert.assertEquals(testFastaUri, referenceDd.getFastaGcsUri());
        Assert.assertEquals(testFaiUri, referenceDd.getFaiGcsUri());
        Assert.assertEquals(testCurrentPath + testAllRefsLocalPath + testDbName + "/" + testFastaName, referenceDd.getFastaLocalPath());
    }
}
