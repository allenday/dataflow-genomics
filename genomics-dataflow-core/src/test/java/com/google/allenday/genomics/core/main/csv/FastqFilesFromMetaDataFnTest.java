package com.google.allenday.genomics.core.main.csv;

import com.google.allenday.genomics.core.csv.FastqFilesFromMetaDataFn;
import com.google.allenday.genomics.core.io.DefaultBaseUriProvider;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;


public class FastqFilesFromMetaDataFnTest implements Serializable {


    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void testFastqFilesFromMetaDataFn() {
        String testBucket = "testBucket";
        String runId = "runId";
        String sample = "sample";
        String study = "study";

        String testBase = "fastq/study/sample/runId_";
        String testBaseUri = "gs://testBucket/" + testBase;

        String testFileName1 = testBase + "R1.fastq";
        String testFileName2 = testBase + "R2.fastq";

        SampleMetaData sampleMetaData = new SampleMetaData();
        sampleMetaData.setRunId(runId);
        sampleMetaData.setSraSample(SraSampleId.create(sample));
        sampleMetaData.setSraStudy(study);
        sampleMetaData.setLibraryLayout("PAIRED");
        sampleMetaData.setSrcRawMetaData("");

        DefaultBaseUriProvider defaultBaseUriProvider = DefaultBaseUriProvider.withDefaultProviderRule(testBucket);
        FileUtils fileUtils = new FileUtils();
        GCSService gcsService = Mockito.mock(GCSService.class, Mockito.withSettings().serializable());
        Mockito.when(gcsService.getBlobIdFromUri(testBaseUri)).thenReturn(BlobId.of(testBucket, testBase));

        BlobId blobId1 = BlobId.of(testBucket, testFileName1);
        BlobId blobId2 = BlobId.of(testBucket, testFileName2);

        Mockito.when(gcsService.getBlobsIdsWithPrefixList(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(Arrays.asList(blobId2, blobId1));

        Mockito.when(gcsService.getUriFromBlob(blobId1))
                .thenReturn(String.format("gs://%s/%s", testBucket, testFileName1));
        Mockito.when(gcsService.getUriFromBlob(blobId2))
                .thenReturn(String.format("gs://%s/%s", testBucket, testFileName2));

        PCollection<KV<SampleMetaData, List<FileWrapper>>> resultPCollection = testPipeline
                .apply(Create.of(sampleMetaData))
                .apply(ParDo.of(new FastqFilesFromMetaDataFn(defaultBaseUriProvider, fileUtils).setGcsService(gcsService)));

        PAssert.that(resultPCollection)
                .satisfies(new SimpleFunction<Iterable<KV<SampleMetaData, List<FileWrapper>>>, Void>() {
                    @Override
                    public Void apply(Iterable<KV<SampleMetaData, List<FileWrapper>>> input) {

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
    }
}