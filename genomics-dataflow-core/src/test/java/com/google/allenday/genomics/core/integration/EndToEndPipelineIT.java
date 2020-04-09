package com.google.allenday.genomics.core.integration;

import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.UriProvider;
import com.google.allenday.genomics.core.model.*;
import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
import com.google.allenday.genomics.core.processing.AlignAndSamProcessingTransform;
import com.google.allenday.genomics.core.processing.SplitFastqIntoBatches;
import com.google.allenday.genomics.core.processing.align.Minimap2AlignService;
import com.google.allenday.genomics.core.processing.sam.SamBamManipulationService;
import com.google.allenday.genomics.core.processing.sam.SamRecordsMetadaKey;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.allenday.genomics.core.utils.ResourceProvider;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Tests genomics data processing in Fastq to merged BAM mode with DirectRunner
 */
public class EndToEndPipelineIT implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(EndToEndPipelineIT.class);

    private final static String DEFAULT_TEST_BUCKET = "human1000-results";
    private final static String REFERENCE_LOCAL_DIR = "reference/";

    private final static String TEST_EXAMPLE_SRA = "SRS0000001";

    private final static String MAIN_TESTING_GCS_DIR = "testing/";

    private final static String TEST_GCS_INPUT_DATA_DIR = MAIN_TESTING_GCS_DIR + "input/";
    private final static String TEST_GCS_REFERENCE_DIR = MAIN_TESTING_GCS_DIR + "reference/";

    private final static int TEST_MAX_FASTQ_CHUNK_SIZE = 5000;
    private final static int TEST_MAX_FASTQ_CONTENT_SIZE_MB = 50;
    private final static int TEST_MAX_SAM_RECORDS_BATCH_SIZE = 1000000;

    private final static String TEST_REFERENCE_NAME = "PRJNA482748_10";
    private final static String TEST_REFERENCE_FILE = "PRJNA482748_10.fa";
    private final static String TEMP_DIR = "temp/";

    private final static List<List<String>> TEST_INPUT_FILES = Arrays.asList(Collections.singletonList("test_single_end_read_5000.fastq"),
            Arrays.asList("test_paired_read_5000_1.fastq", "test_paired_read_5000_2.fastq"));

    private final static String TEST_CSV_FILE = "source.csv";
    private final static String EXPECTED_SINGLE_END_RESULT_CONTENT_FILE = "expected_result_5k.merged.sorted.bam";

    private final static String CSV_LINE_TEMPLATE = "\t\t\t\t\t\t%1$s\t\t\t\t%2$s\t%3$s\t\t\t\t\t\t\t\t\t\t\t\t%4$s\t\t ";

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

        String testBucket = Optional
                .ofNullable(System.getenv("TEST_BUCKET"))
                .orElse(DEFAULT_TEST_BUCKET);

        FileUtils fileUtils = new FileUtils();
        GCSService gcsService = GCSService.initialize(fileUtils);
        ResourceProvider resourceProvider = ResourceProvider.initialize();
        Pair<String, UriProvider> inputCsvUriAndProvider = prepareInputData(gcsService, fileUtils, testBucket, TEST_INPUT_FILES, TEST_CSV_FILE);
        String allReferencesDirGcsUri = prepareReference(gcsService, fileUtils, testBucket);

        GenomicsOptions genomicsOptions = new GenomicsOptions(
                Aligner.MINIMAP2,
                testBucket,
                Collections.singletonList(TEST_REFERENCE_NAME),
                allReferencesDirGcsUri,
                ValueProvider.StaticValueProvider.of(null),
                VariantCaller.GATK,
                MAIN_TESTING_GCS_DIR,
                0
        );
        EndToEndPipelineITModule endToEndPipelineITModule = new EndToEndPipelineITModule(
                testBucket,
                inputCsvUriAndProvider.getValue0(),
                Collections.emptyList(),
                Collections.emptyList(),
                resourceProvider.getProjectId(),
                "us-central",
                genomicsOptions,
                TEST_MAX_FASTQ_CONTENT_SIZE_MB,
                TEST_MAX_FASTQ_CHUNK_SIZE,
                UriProvider.FastqExt.defaultExt(),
                TEST_MAX_SAM_RECORDS_BATCH_SIZE,
                inputCsvUriAndProvider.getValue1(),
                true);

        Injector injector = Guice.createInjector(endToEndPipelineITModule);

        pipeline
                .apply("Parse data", injector.getInstance(ParseSourceCsvTransform.class))
                .apply("Split large FASTQ into chunks", injector.getInstance(SplitFastqIntoBatches.class))
                .apply("Align reads and prepare for DV", injector.getInstance(AlignAndSamProcessingTransform.class));

//        TODO DeepVeariant temporary excluded from end-to-end tests
//                .apply(ParDo.of(new VariantCallingFn(deepVariantService, dvResultGcsPath)))
        ;

        PipelineResult pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        NameProvider nameProvider = injector.getInstance(NameProvider.class);
        List<BlobId> mergeResults = getBlobIdsWithDirAndEnding(gcsService, testBucket,
                MAIN_TESTING_GCS_DIR + String.format(GenomicsOptions.MERGED_REGIONS_PATH_PATTERN, nameProvider.getCurrentTimeInDefaultFormat()),
                ".merged.sorted.bam");
        List<BlobId> indexResults = getBlobIdsWithDirAndEnding(gcsService, testBucket,
                MAIN_TESTING_GCS_DIR + String.format(GenomicsOptions.MERGED_REGIONS_PATH_PATTERN, nameProvider.getCurrentTimeInDefaultFormat())
                , ".merged.sorted.bam.bai");

        BlobId finalMergeBlobId = BlobId.of(testBucket,
                MAIN_TESTING_GCS_DIR + String.format(GenomicsOptions.FINAL_MERGED_PATH_PATTERN, nameProvider.getCurrentTimeInDefaultFormat()) +
                        new SamRecordsMetadaKey(SraSampleId.create(TEST_EXAMPLE_SRA), TEST_REFERENCE_NAME, SamRecordsMetadaKey.Region.UNDEFINED).generateSlug() + ".merged.sorted.bam");

        Assert.assertEquals(mergeResults.size(), indexResults.size());
        checkResultContent(gcsService, fileUtils, finalMergeBlobId);
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

            String csvLine = String.format(CSV_LINE_TEMPLATE,
                    libraryLayout, fileNameBase, TEST_EXAMPLE_SRA, Instrument.ILLUMINA.name());
            csvLines.append(csvLine).append("\n");

        });
        Blob blob = gcsService.writeToGcs(bucketName, TEST_GCS_INPUT_DATA_DIR + testInputCsvFileName,
                Channels.newChannel(new ByteArrayInputStream(csvLines.toString().getBytes())));
        UriProvider uriProvider = new UriProvider(bucketName, (UriProvider.ProviderRule) (geneSampleMetaData, srcBucket) -> {
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
        Assert.assertTrue(String.format("File %s not exists", expectedResultBlob.toString()), resultExists);
    }

    private void checkResultContent(GCSService gcsService, FileUtils fileUtils, BlobId expectedResultBlob) throws IOException {
        checkExists(gcsService, expectedResultBlob);
        String destFileName = TEMP_DIR + expectedResultBlob.getName();
        fileUtils.mkdirFromUri(destFileName);

        gcsService.downloadBlobTo(gcsService.getBlob(expectedResultBlob), destFileName);

        SamBamManipulationService samBamManipulationService = new SamBamManipulationService(fileUtils);

        File expectedResultsFile = new File(getClass().getClassLoader().getResource(EXPECTED_SINGLE_END_RESULT_CONTENT_FILE).getFile());
        File actualResultsFile = new File(fileUtils.getCurrentPath() + destFileName);


        boolean isTwoEqual = samBamManipulationService.isRecordsInBamEquals(expectedResultsFile, actualResultsFile);
        Assert.assertTrue("Result content is not equals with expected", isTwoEqual);
    }

    private List<BlobId> getBlobIdsWithDirAndEnding(GCSService gcsService, String bucket, String dir, String ending) {
        return StreamSupport.stream(gcsService.getListOfBlobsInDir(bucket, dir).iterateAll().spliterator(), false)
                .map(Blob::getBlobId).filter(blobId -> blobId.getName().endsWith(ending)).collect(Collectors.toList());
    }


    @After
    public void finalizeTests() {
        FileUtils fileUtils = new FileUtils();
        fileUtils.deleteDir(Minimap2AlignService.MINIMAP_NAME);
        fileUtils.deleteDir(TEMP_DIR);
        fileUtils.deleteDir(REFERENCE_LOCAL_DIR);

        GCSService gcsService = GCSService.initialize(fileUtils);
        String testBucket = Optional
                .ofNullable(System.getenv("TEST_BUCKET"))
                .orElse(DEFAULT_TEST_BUCKET);
        gcsService.getListOfBlobsInDir(testBucket, MAIN_TESTING_GCS_DIR)
                .iterateAll().forEach(blob -> {
            LOG.info(String.format("Deleting: %s ...", blob.getBlobId().toString()));
            blob.delete();
        });
    }
}