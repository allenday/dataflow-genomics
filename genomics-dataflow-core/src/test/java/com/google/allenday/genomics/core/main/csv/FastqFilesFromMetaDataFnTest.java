package com.google.allenday.genomics.core.main.csv;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;

import java.io.Serializable;


public class FastqFilesFromMetaDataFnTest implements Serializable {


    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

   /* @Test
    public void testFastqFilesFromMetaDataFn() {
        String testBucket = "testBucket";
        String runId = "runId";
        String sample = "sample";
        String study = "study";

        String testBase = "runfile/study/sample/runId";
        String testBaseUri = "gs://testBucket/" + testBase;

        String testFileName1 = testBase + "R1.runfile";
        String testFileName2 = testBase + "R2.runfile";

        SampleRunMetaData sampleMetaData = new SampleRunMetaData();
        sampleMetaData.setRunId(runId);
        sampleMetaData.setSraSample(SraSampleId.create(sample));
        sampleMetaData.setSraStudy(study);
        sampleMetaData.setLibraryLayout("PAIRED");
        sampleMetaData.setRawMetaData("");

        DefaultBaseUriProvider defaultBaseUriProvider = DefaultBaseUriProvider.withDefaultProviderRule(testBucket);
        FileUtils fileUtils = new FileUtils();
        GcsService gcsService = Mockito.mock(GcsService.class, Mockito.withSettings().serializable());
        Mockito.when(gcsService.getBlobIdFromUri(testBaseUri)).thenReturn(BlobId.of(testBucket, testBase));

        BlobId blobId1 = BlobId.of(testBucket, testFileName1);
        BlobId blobId2 = BlobId.of(testBucket, testFileName2);

        Mockito.when(gcsService.getBlobsIdsWithPrefixList(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(Arrays.asList(blobId2, blobId1));

        Mockito.when(gcsService.getUriFromBlob(blobId1))
                .thenReturn(String.format("gs://%s/%s", testBucket, testFileName1));
        Mockito.when(gcsService.getUriFromBlob(blobId2))
                .thenReturn(String.format("gs://%s/%s", testBucket, testFileName2));

        PCollection<KV<SampleRunMetaData, List<FileWrapper>>> resultPCollection = testPipeline
                .apply(Create.of(sampleMetaData))
                .apply(ParDo.of(new FastqFilesFromMetaDataFn(defaultBaseUriProvider, fileUtils).setGcsService(gcsService)));

        PAssert.that(resultPCollection)
                .satisfies(new SimpleFunction<Iterable<KV<SampleRunMetaData, List<FileWrapper>>>, Void>() {
                    @Override
                    public Void apply(Iterable<KV<SampleRunMetaData, List<FileWrapper>>> input) {

                        StreamSupport.stream(input.spliterator(), false)
                                .findFirst().ifPresent(kv -> {
                            List<FileWrapper> value = kv.getValue();
                            Assert.assertTrue(value.get(0).getBlobUri().endsWith(testFileName1));
                            Assert.assertTrue(value.get(1).getBlobUri().endsWith(testFileName2));
                        });
                        return null;
                    }

                });
        PipelineResult pipelineResult = testPipeline.run();
        pipelineResult.waitUntilFinish();
    }*/
}