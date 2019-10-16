package com.google.allenday.genomics.core.integration;

import com.google.allenday.genomics.core.align.AlignService;
import com.google.allenday.genomics.core.align.SamBamManipulationService;
import com.google.allenday.genomics.core.align.transform.AlignFn;
import com.google.allenday.genomics.core.align.transform.AlignSortMergeTransform;
import com.google.allenday.genomics.core.align.transform.MergeFn;
import com.google.allenday.genomics.core.align.transform.SortFn;
import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.gene.UriProvider;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.TestPipeline;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Tests full pipeline lifecycle in DataflowRunner mode
 */
public class EndToEndPipelineTest implements Serializable {

    private final static String REFERENCE_LOCAL_DIR = "reference/";

    private final static String TEST_EXAMPLE_SRA = "SRS0000001";

    private final static String ALIGN_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_aligned_bam/";
    private final static String SORT_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_sorted_bam/";
    private final static String MERGE_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_merged_bam/";

    private final static String TEST_GCS_INPUT_DATA_DIR = "testing/input/";
    private final static String TEST_GCS_REFERENCE_DIR = "testing/reference/";

    private final static String TEST_REFERENCE_NAME = "PRJNA482748_QRJC01.1";
    private final static String TEST_REFERENCE_FILE = "PRJNA482748_QRJC01.1.fsa_nt";
    private final static String TEST_REFERENCE_FILE_EXTENSION = ".fsa_nt";
    private final static String TEMP_DIR = "temp/";

    private final static String TEST_SINGLE_END_INPUT_FILE = "test_read_10.bfast.fastq";
    private final static String TEST_CSV_FILE = "source.csv";
    private final static String EXPECTED_SINGLE_END_RESULT_CONTENT_FILE = "expected_single_end_result.merged.sorted.bam";

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void testEndToEndPipelineWithSingleEnd() throws IOException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd--HH-mm-ss-z");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String jobTime = simpleDateFormat.format(new Date());

        FileUtils fileUtils = new FileUtils();
        String testBucket = "cannabis-3k-results";

        GeneExampleMetaData testGeneExampleMetaData =
                new GeneExampleMetaData("TestProject", "TestProjectId", "TestBioSample",
                        TEST_EXAMPLE_SRA, "TestRun", false, "testSrcRawMetaData");

        GCSService gcsService = GCSService.initialize(fileUtils);
        Pair<String, UriProvider> inputCsvUriAndProvider = prepareInputData(gcsService, fileUtils, testBucket, TEST_SINGLE_END_INPUT_FILE, TEST_CSV_FILE);
        String allReferencesDirGcsUri = prepareReference(gcsService, fileUtils, testBucket);

        CmdExecutor cmdExecutor = new CmdExecutor();
        SamBamManipulationService samBamManipulationService = new SamBamManipulationService(fileUtils);

        TransformIoHandler alignTransformIoHandler = new TransformIoHandler(testBucket, String.format(ALIGN_RESULT_GCS_DIR_PATH_PATTERN, jobTime), 300, fileUtils);
        TransformIoHandler sortTransformIoHandler = new TransformIoHandler(testBucket, String.format(SORT_RESULT_GCS_DIR_PATH_PATTERN, jobTime), 300, fileUtils);
        TransformIoHandler mergeTransformIoHandler = new TransformIoHandler(testBucket, String.format(MERGE_RESULT_GCS_DIR_PATH_PATTERN, jobTime), 300, fileUtils);

        AlignFn alignFn = new AlignFn(new AlignService(new WorkerSetupService(cmdExecutor), cmdExecutor, fileUtils),
                new ReferencesProvider(fileUtils, allReferencesDirGcsUri, REFERENCE_LOCAL_DIR, TEST_REFERENCE_FILE_EXTENSION),
                Collections.singletonList(TEST_REFERENCE_NAME), alignTransformIoHandler, fileUtils);
        SortFn sortFn = new SortFn(sortTransformIoHandler, fileUtils, samBamManipulationService);
        MergeFn mergeFn = new MergeFn(mergeTransformIoHandler, samBamManipulationService, fileUtils);

        String mergeResultGcsPath = String.format(MERGE_RESULT_GCS_DIR_PATH_PATTERN, jobTime);

        testPipeline
                .apply(new ParseSourceCsvTransform(inputCsvUriAndProvider.getValue0(),
                        GeneExampleMetaData.Parser.withDefaultSchema(),
                        inputCsvUriAndProvider.getValue1(), fileUtils))
                .apply(new AlignSortMergeTransform("AlignSortMergeTransform", alignFn, sortFn, mergeFn));

        PipelineResult pipelineResult = testPipeline.run();
        pipelineResult.waitUntilFinish();

        BlobId expectedResultBlob = BlobId.of(testBucket, mergeResultGcsPath + TEST_EXAMPLE_SRA + "_" + TEST_REFERENCE_NAME + ".merged.sorted.bam");
        checkResultContent(gcsService, fileUtils, expectedResultBlob);
    }

    private Pair<String, UriProvider> prepareInputData(GCSService gcsService, FileUtils fileUtils, String bucketName, String testInputDataFile, String testInputCsvFileName) throws IOException {
        gcsService.writeToGcs(bucketName, TEST_GCS_INPUT_DATA_DIR + testInputDataFile,
                Channels.newChannel(getClass().getClassLoader().getResourceAsStream(testInputDataFile)));
        String csvLine = String.join(",", new String[]{"TestProject", "TestProjectId", "TestBioSample",
                TEST_EXAMPLE_SRA, "test_read_10", "", "SINGLE", "testSrcRawMetaData"});
        Blob blob = gcsService.writeToGcs(bucketName, TEST_GCS_INPUT_DATA_DIR + testInputCsvFileName,
                Channels.newChannel(new ByteArrayInputStream(csvLine.getBytes())));
        UriProvider uriProvider = new UriProvider(bucketName, new UriProvider.ProviderRule() {
            @Override
            public List<String> provideAccordinglyRule(GeneExampleMetaData geneExampleMetaData, String srcBucket) {
                return Collections.singletonList(String.format("gs://%s/%s", srcBucket,
                        TEST_GCS_INPUT_DATA_DIR + geneExampleMetaData.getRunId() + ".bfast.fastq"));
            }
        });
        return Pair.with(gcsService.getUriFromBlob(blob.getBlobId()), uriProvider);
    }

    private String prepareReference(GCSService gcsService, FileUtils fileUtils, String bucketName) throws IOException {
        Blob blob = gcsService.writeToGcs(bucketName, TEST_GCS_REFERENCE_DIR + TEST_REFERENCE_FILE,
                Channels.newChannel(getClass().getClassLoader().getResourceAsStream(TEST_REFERENCE_FILE)));
        return gcsService.getUriFromBlob(BlobId.of(bucketName, TEST_GCS_REFERENCE_DIR));
    }

    private void checkResultContent(GCSService gcsService, FileUtils fileUtils, BlobId expectedResultBlob) throws IOException {
        boolean resultExists = gcsService.isExists(expectedResultBlob);
        Assert.assertTrue("Results file exists", resultExists);

        String destFileName = TEMP_DIR + expectedResultBlob.getName();

        fileUtils.mkdir(destFileName);
        gcsService.downloadBlobTo(gcsService.getBlob(expectedResultBlob), destFileName);

        File expectedResultsFile = new File(getClass().getClassLoader().getResource(EXPECTED_SINGLE_END_RESULT_CONTENT_FILE).getFile());
        File actualResultsFile = new File(fileUtils.getCurrentPath() + destFileName);

        SamBamManipulationService samBamManipulationService = new SamBamManipulationService(fileUtils);

        boolean isTwoEqual = samBamManipulationService.isRecordsInBamEquals(expectedResultsFile, actualResultsFile);
        Assert.assertTrue("Results content is equals to expected", isTwoEqual);
    }

    @After
    public void finalizeTests() {
        FileUtils fileUtils = new FileUtils();

        fileUtils.deleteDir(AlignService.MINIMAP_NAME);
        fileUtils.deleteDir(TEMP_DIR);
        fileUtils.deleteDir(REFERENCE_LOCAL_DIR);
    }

}