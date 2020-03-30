package com.google.allenday.genomics.core.integration;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.io.*;
import com.google.allenday.genomics.core.model.Instrument;
import com.google.allenday.genomics.core.model.Instrument;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.pipeline.DeepVariantOptions;
import com.google.allenday.genomics.core.processing.AlignAndPostProcessTransform;
import com.google.allenday.genomics.core.processing.SplitFastqIntoBatches;
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
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Tests genomics data processing in Fastq to merged BAM mode with DirectRunner
 */
public class EndToEndPipelineIT implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(EndToEndPipelineIT.class);

    private final static String REFERENCE_LOCAL_DIR = "reference/";

    private final static String TEST_EXAMPLE_SRA = "SRS0000001";

    private final static String FASTQ_SIZE_CHUNK_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_fastq_size_chunk/";
    private final static String FASTQ_COUNT_CHUNK_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_fastq_count_chunk/";
    private final static String ALIGN_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_aligned_bam/";
    private final static String SORT_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_sorted_bam/";
    private final static String MERGE_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_merged_bam/";
    private final static String INDEX_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_merged_bam/";
    private final static String DV_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_dv/";

    private final static String TEST_GCS_INPUT_DATA_DIR = "testing/input/";
    private final static String TEST_GCS_REFERENCE_DIR = "testing/reference/";

    private final static int TEST_MAX_FASTQ_CHUNK_SIZE = 1000;
    private final static int TEST_MAX_FASTQ_CONTENT_SIZE_MB = 50;

    private final static String TEST_REFERENCE_NAME = "PRJNA482748_10";
    private final static String TEST_REFERENCE_FILE = "PRJNA482748_10.fa";
    private final static String TEMP_DIR = "temp/";

    private final static List<List<String>> TEST_INPUT_FILES = Arrays.asList(Collections.singletonList("test_single_end_read_5000.fastq"),
            Arrays.asList("test_paired_read_5000_1.fastq", "test_paired_read_5000_2.fastq"));

    private final static String TEST_CSV_FILE = "source.csv";
    private final static String EXPECTED_SINGLE_END_RESULT_CONTENT_FILE = "expected_result_5k.merged.sorted.bam";

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
    public void testEndToEndPipeline() throws IOException {
        DirectOptions directOptions = PipelineOptionsFactory
                .as(DirectOptions.class);
        directOptions.setTargetParallelism(1);
        Pipeline pipeline = Pipeline.create(directOptions);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd--HH-mm-ss-z");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String jobTime = simpleDateFormat.format(new Date());

        FileUtils fileUtils = new FileUtils();
        IoUtils ioUtils = new IoUtils();
        FastqReader fastqReader = new FastqReader();
        String testBucket = Optional
                .ofNullable(System.getenv("TEST_BUCKET"))
                .orElse("cannabis-3k-results");

        GCSService gcsService = GCSService.initialize(fileUtils);
        Pair<String, UriProvider> inputCsvUriAndProvider = prepareInputData(gcsService, fileUtils, testBucket, TEST_INPUT_FILES, TEST_CSV_FILE);
        String allReferencesDirGcsUri = prepareReference(gcsService, fileUtils, testBucket);

        CmdExecutor cmdExecutor = new CmdExecutor();
        SamBamManipulationService samBamManipulationService = new SamBamManipulationService(fileUtils);

        String mergeResultGcsPath = String.format(MERGE_RESULT_GCS_DIR_PATH_PATTERN, jobTime);
        String indexResultGcsPath = String.format(INDEX_RESULT_GCS_DIR_PATH_PATTERN, jobTime);

        String dvResultGcsPath = gcsService.getUriFromBlob(BlobId.of(testBucket, String.format(DV_RESULT_GCS_DIR_PATH_PATTERN, jobTime)));

        ReferenceProvider referencesProvider = new ReferenceProvider(fileUtils);
        DeepVariantService deepVariantService = new DeepVariantService(new LifeSciencesService(), new DeepVariantOptions());

        TransformIoHandler splitFastqIntoBatchesIoHandler = new TransformIoHandler(
                testBucket, String.format(FASTQ_COUNT_CHUNK_RESULT_GCS_DIR_PATH_PATTERN, jobTime), fileUtils);
        TransformIoHandler buildFastqContentIoHandler = new TransformIoHandler(
                testBucket, String.format(FASTQ_SIZE_CHUNK_RESULT_GCS_DIR_PATH_PATTERN, jobTime), fileUtils);
        TransformIoHandler alignTransformIoHandler =
                new TransformIoHandler(testBucket, String.format(ALIGN_RESULT_GCS_DIR_PATH_PATTERN, jobTime), fileUtils);
        TransformIoHandler sortTransformIoHandler =
                new TransformIoHandler(testBucket, String.format(SORT_RESULT_GCS_DIR_PATH_PATTERN, jobTime), fileUtils);
        TransformIoHandler mergeTransformIoHandler =
                new TransformIoHandler(testBucket, mergeResultGcsPath, fileUtils);
        TransformIoHandler indexTransformIoHandler =
                new TransformIoHandler(testBucket, indexResultGcsPath, fileUtils);

        SplitFastqIntoBatches.ReadFastqPartFn readFastqPartFn =
                new SplitFastqIntoBatches.ReadFastqPartFn(fileUtils, fastqReader, splitFastqIntoBatchesIoHandler,
                        TEST_MAX_FASTQ_CHUNK_SIZE, TEST_MAX_FASTQ_CONTENT_SIZE_MB);
        SplitFastqIntoBatches.BuildFastqContentFn buildFastqContentFn =
                new SplitFastqIntoBatches.BuildFastqContentFn(buildFastqContentIoHandler, fileUtils, ioUtils, TEST_MAX_FASTQ_CONTENT_SIZE_MB);

        AlignFn alignFn = new AlignFn(new AlignService(new WorkerSetupService(cmdExecutor), cmdExecutor, fileUtils),
                referencesProvider,
                alignTransformIoHandler, fileUtils);

        AddReferenceDataSourceFn.FromNameAndDirPath addReferenceDataSourceFn = new AddReferenceDataSourceFn.FromNameAndDirPath(
                allReferencesDirGcsUri, Collections.singletonList(TEST_REFERENCE_NAME));

        AlignTransform alignTransform = new AlignTransform("Align reads transform", alignFn, addReferenceDataSourceFn);
        SortFn sortFn = new SortFn(sortTransformIoHandler, samBamManipulationService, fileUtils);
        MergeFn mergeFn = new MergeFn(mergeTransformIoHandler, samBamManipulationService, fileUtils);
        CreateBamIndexFn createBamIndexFn = new CreateBamIndexFn(indexTransformIoHandler, samBamManipulationService, fileUtils);

        pipeline
                .apply(new ParseSourceCsvTransform(inputCsvUriAndProvider.getValue0(),
                        new TestSraParser(SampleMetaData.Parser.Separation.COMMA),
                        inputCsvUriAndProvider.getValue1(), fileUtils))
                .apply(new SplitFastqIntoBatches(readFastqPartFn, buildFastqContentFn, TEST_MAX_FASTQ_CONTENT_SIZE_MB))
                .apply(new AlignAndPostProcessTransform("AlignAndPostProcessTransform", alignTransform, sortFn, mergeFn, createBamIndexFn))

//        TODO DeepVeariant temporary excluded from end-to-end tests
//                .apply(ParDo.of(new DeepVariantFn(deepVariantService, dvResultGcsPath)))
        ;

        PipelineResult pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        BlobId expectedResultMergeBlob = BlobId.of(testBucket, mergeResultGcsPath + TEST_EXAMPLE_SRA + "_" + TEST_REFERENCE_NAME + ".merged.sorted.bam");
        BlobId expectedResultIndexBlob = BlobId.of(testBucket, indexResultGcsPath + TEST_EXAMPLE_SRA + "_" + TEST_REFERENCE_NAME + ".merged.sorted.bam.bai");
        checkExists(gcsService, expectedResultIndexBlob);
        checkResultContent(gcsService, fileUtils, expectedResultMergeBlob);
    }

    private Pair<String, UriProvider> prepareInputData(GCSService gcsService, FileUtils fileUtils, String bucketName,
                                                       List<List<String>> testInputDataFiles, String testInputCsvFileName) throws IOException {

        StringBuilder csvLines = new StringBuilder();
        testInputDataFiles.forEach(list -> {
            list.forEach(filename -> {
                try {
                    gcsService.writeToGcs(bucketName, TEST_GCS_INPUT_DATA_DIR + filename,
                            Channels.newChannel(getClass().getClassLoader().getResourceAsStream(filename)));
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            });
            String fileName = list.get(0);
            Pair<String, String> filenameAndExtension = fileUtils.splitFilenameAndExtension(fileName);
            String fileNameBase = filenameAndExtension.getValue0();
            if (fileNameBase.endsWith("_1")) {
                fileNameBase = fileNameBase.substring(0, fileNameBase.length() - 2);
            }
            String libraryLayout = list.size() == 2 ? "PAIRED" : "SINGLE";

            String csvLine = String.join(",", new String[]{
                    TEST_EXAMPLE_SRA, fileNameBase, libraryLayout, Instrument.ILLUMINA.name()});
            csvLines.append(csvLine).append("\n");

        });
        Blob blob = gcsService.writeToGcs(bucketName, TEST_GCS_INPUT_DATA_DIR + testInputCsvFileName,
                Channels.newChannel(new ByteArrayInputStream(csvLines.toString().getBytes())));
        UriProvider uriProvider = new UriProvider(bucketName, new UriProvider.ProviderRule() {
            @Override
            public List<String> provideAccordinglyRule(SampleMetaData geneSampleMetaData, String srcBucket) {
                if (geneSampleMetaData.isPaired()) {
                    String uri1 = String.format("gs://%s/%s", srcBucket,
                            TEST_GCS_INPUT_DATA_DIR + geneSampleMetaData.getRunId() + "_1" + ".fastq");
                    String uri2 = String.format("gs://%s/%s", srcBucket,
                            TEST_GCS_INPUT_DATA_DIR + geneSampleMetaData.getRunId() + "_2" + ".fastq");
                    return Arrays.asList(uri1, uri2);
                } else {
                    return Collections.singletonList(String.format("gs://%s/%s", srcBucket,
                            TEST_GCS_INPUT_DATA_DIR + geneSampleMetaData.getRunId() + ".fastq"));
                }
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