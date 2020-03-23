package com.google.allenday.genomics.core.integration;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.io.UriProvider;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.pipeline.DeepVariantOptions;
import com.google.allenday.genomics.core.processing.AlignAndPostProcessTransform;
import com.google.allenday.genomics.core.processing.align.AddReferenceDataSourceFn;
import com.google.allenday.genomics.core.processing.align.AlignFn;
import com.google.allenday.genomics.core.processing.align.AlignService;
import com.google.allenday.genomics.core.processing.align.AlignTransform;
import com.google.allenday.genomics.core.processing.dv.DeepVariantService;
import com.google.allenday.genomics.core.processing.lifesciences.LifeSciencesService;
import com.google.allenday.genomics.core.processing.sam.CreateBamIndexFn;
import com.google.allenday.genomics.core.processing.sam.MergeFn;
import com.google.allenday.genomics.core.processing.sam.SamBamManipulationService;
import com.google.allenday.genomics.core.processing.sam.SortFn;
import com.google.allenday.genomics.core.reference.ReferenceProvider;
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
import java.util.*;

/**
 * Tests full pipeline lifecycle in DirectRunner mode
 */
public class EndToEndPipelineIT implements Serializable {

    private final static String REFERENCE_LOCAL_DIR = "reference/";

    private final static String TEST_EXAMPLE_SRA = "SRS0000001";

    private final static String ALIGN_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_aligned_bam/";
    private final static String SORT_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_sorted_bam/";
    private final static String MERGE_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_merged_bam/";
    private final static String INDEX_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_merged_bam/";
    private final static String DV_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_dv/";

    private final static String TEST_GCS_INPUT_DATA_DIR = "testing/input/";
    private final static String TEST_GCS_REFERENCE_DIR = "testing/reference/";

    private final static String TEST_REFERENCE_NAME = "PRJNA482748_10";
    private final static String TEST_REFERENCE_FILE = "PRJNA482748_10.fa";
    private final static String TEMP_DIR = "temp/";

    private final static String TEST_SINGLE_END_INPUT_FILE = "test_read_10000.bfast.fastq";
    private final static String TEST_CSV_FILE = "source.csv";
    private final static String EXPECTED_SINGLE_END_RESULT_CONTENT_FILE = "expected_single_end_result.merged.sorted.bam";

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    public class TestSraParser extends SampleMetaData.Parser {

        public TestSraParser(Separation separation) {
            super(separation);
        }

        @Override
        public SampleMetaData processParts(String[] csvLineParts, String csvLine) throws CsvParseException {
            return new SampleMetaData(csvLineParts[0], csvLineParts[1], csvLineParts[2], csvLineParts[3], csvLine);
        }
    }

    @Test
    public void testEndToEndPipelineWithSingleEnd() throws IOException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd--HH-mm-ss-z");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String jobTime = simpleDateFormat.format(new Date());

        FileUtils fileUtils = new FileUtils();
        String testBucket = Optional
                .ofNullable(System.getenv("TEST_BUCKET"))
                .orElse("cannabis-3k-results");

        GCSService gcsService = GCSService.initialize(fileUtils);
        Pair<String, UriProvider> inputCsvUriAndProvider = prepareInputData(gcsService, fileUtils, testBucket, TEST_SINGLE_END_INPUT_FILE, TEST_CSV_FILE);
        String allReferencesDirGcsUri = prepareReference(gcsService, fileUtils, testBucket);

        CmdExecutor cmdExecutor = new CmdExecutor();
        SamBamManipulationService samBamManipulationService = new SamBamManipulationService(fileUtils);

        String mergeResultGcsPath = String.format(MERGE_RESULT_GCS_DIR_PATH_PATTERN, jobTime);
        String indexResultGcsPath = String.format(INDEX_RESULT_GCS_DIR_PATH_PATTERN, jobTime);

        String dvResultGcsPath = gcsService.getUriFromBlob(BlobId.of(testBucket, String.format(DV_RESULT_GCS_DIR_PATH_PATTERN, jobTime)));

        ReferenceProvider referencesProvider = new ReferenceProvider(fileUtils);
        DeepVariantService deepVariantService = new DeepVariantService(new LifeSciencesService(), new DeepVariantOptions());

        TransformIoHandler alignTransformIoHandler = new TransformIoHandler(testBucket, String.format(ALIGN_RESULT_GCS_DIR_PATH_PATTERN, jobTime), 300, fileUtils);
        TransformIoHandler sortTransformIoHandler = new TransformIoHandler(testBucket, String.format(SORT_RESULT_GCS_DIR_PATH_PATTERN, jobTime), 300, fileUtils);
        TransformIoHandler mergeTransformIoHandler = new TransformIoHandler(testBucket, mergeResultGcsPath, 0, fileUtils);
        TransformIoHandler indexTransformIoHandler = new TransformIoHandler(testBucket, indexResultGcsPath, 0, fileUtils);

        AlignFn alignFn = new AlignFn(new AlignService(new WorkerSetupService(cmdExecutor), cmdExecutor, fileUtils),
                referencesProvider,
                alignTransformIoHandler, fileUtils);

        AddReferenceDataSourceFn.FromNameAndDirPath addReferenceDataSourceFn = new AddReferenceDataSourceFn.FromNameAndDirPath(
                allReferencesDirGcsUri, Collections.singletonList(TEST_REFERENCE_NAME));

        AlignTransform alignTransform = new AlignTransform("Align reads transform", alignFn, addReferenceDataSourceFn);
        SortFn sortFn = new SortFn(sortTransformIoHandler, samBamManipulationService, fileUtils);
        MergeFn mergeFn = new MergeFn(mergeTransformIoHandler, samBamManipulationService, fileUtils);
        CreateBamIndexFn createBamIndexFn = new CreateBamIndexFn(indexTransformIoHandler, samBamManipulationService, fileUtils);

        testPipeline
                .apply(new ParseSourceCsvTransform(inputCsvUriAndProvider.getValue0(),
                        new TestSraParser(SampleMetaData.Parser.Separation.COMMA),
                        inputCsvUriAndProvider.getValue1(), fileUtils))
                .apply(new AlignAndPostProcessTransform("AlignAndPostProcessTransform", alignTransform, sortFn, mergeFn, createBamIndexFn))

//        TODO DeepVeariant temporary excluded from end-to-end tests
//                .apply(ParDo.of(new DeepVariantFn(deepVariantService, dvResultGcsPath)))
        ;

        PipelineResult pipelineResult = testPipeline.run();
        pipelineResult.waitUntilFinish();

        BlobId expectedResultMergeBlob = BlobId.of(testBucket, mergeResultGcsPath + TEST_EXAMPLE_SRA + "_" + TEST_REFERENCE_NAME + ".merged.sorted.bam");
        BlobId expectedResultIndexBlob = BlobId.of(testBucket, indexResultGcsPath + TEST_EXAMPLE_SRA + "_" + TEST_REFERENCE_NAME + ".merged.sorted.bam.bai");
        checkExists(gcsService, expectedResultIndexBlob);
        checkResultContent(gcsService, fileUtils, expectedResultMergeBlob);
    }

    private Pair<String, UriProvider> prepareInputData(GCSService gcsService, FileUtils fileUtils, String bucketName, String testInputDataFile, String testInputCsvFileName) throws IOException {
        gcsService.writeToGcs(bucketName, TEST_GCS_INPUT_DATA_DIR + testInputDataFile,
                Channels.newChannel(getClass().getClassLoader().getResourceAsStream(testInputDataFile)));
        String csvLine = String.join(",", new String[]{
                TEST_EXAMPLE_SRA, "test_read_10000", "SINGLE", AlignService.Instrument.ILLUMINA.name()});
        Blob blob = gcsService.writeToGcs(bucketName, TEST_GCS_INPUT_DATA_DIR + testInputCsvFileName,
                Channels.newChannel(new ByteArrayInputStream(csvLine.getBytes())));
        UriProvider uriProvider = new UriProvider(bucketName, new UriProvider.ProviderRule() {
            @Override
            public List<String> provideAccordinglyRule(SampleMetaData geneSampleMetaData, String srcBucket) {
                return Collections.singletonList(String.format("gs://%s/%s", srcBucket,
                        TEST_GCS_INPUT_DATA_DIR + geneSampleMetaData.getRunId() + ".bfast.fastq"));
            }
        });
        return Pair.with(gcsService.getUriFromBlob(blob.getBlobId()), uriProvider);
    }

    private String prepareReference(GCSService gcsService, FileUtils fileUtils, String bucketName) throws IOException {
        String refName = TEST_REFERENCE_FILE;
        String refIndexName = TEST_REFERENCE_FILE + ".fai";

        Blob blobRef = gcsService.writeToGcs(bucketName, TEST_GCS_REFERENCE_DIR + refName,
                Channels.newChannel(getClass().getClassLoader().getResourceAsStream(refName)));
        Blob blobIndexRef = gcsService.writeToGcs(bucketName, TEST_GCS_REFERENCE_DIR + refIndexName,
                Channels.newChannel(getClass().getClassLoader().getResourceAsStream(refIndexName)));
        return gcsService.getUriFromBlob(BlobId.of(bucketName, TEST_GCS_REFERENCE_DIR));
    }

    private void checkExists(GCSService gcsService, BlobId expectedResultBlob) throws IOException {
        boolean resultExists = gcsService.isExists(expectedResultBlob);
        Assert.assertTrue("Results file exists", resultExists);
    }

    private void checkResultContent(GCSService gcsService, FileUtils fileUtils, BlobId expectedResultBlob) throws IOException {
        checkExists(gcsService, expectedResultBlob);

        String destFileName = TEMP_DIR + expectedResultBlob.getName();

        fileUtils.mkdirFromUri(destFileName);
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