package com.google.allenday.genomics.core.processing.sort;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.processing.SamToolsService;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortFn extends DoFn<KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>,
        KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(SortFn.class);

    private Counter errorCounter = Metrics.counter(SortFn.class, "sort-error-counter");
    private Counter successCounter = Metrics.counter(SortFn.class, "sort-success-counter");

    private GCSService gcsService;

    private TransformIoHandler transformIoHandler;
    private FileUtils fileUtils;
    private SamToolsService samToolsService;

    public SortFn(TransformIoHandler transformIoHandler, SamToolsService samToolsService, FileUtils fileUtils) {
        this.transformIoHandler = transformIoHandler;
        this.fileUtils = fileUtils;
        this.samToolsService = samToolsService;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of sort with input: %s", c.element().toString()));

        KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>> input = c.element();
        ReferenceDatabaseSource referenceDatabaseSource = input.getValue().getKey();
        SampleMetaData geneSampleMetaData = input.getKey();
        FileWrapper fileWrapper = input.getValue().getValue();

        if (geneSampleMetaData == null || fileWrapper == null) {
            LOG.error("Data error");
            LOG.error("geneSampleMetaData: " + geneSampleMetaData);
            LOG.error("fileWrapper: " + fileWrapper);
            throw new RuntimeException("Broken data");
        }
        String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(geneSampleMetaData.getRunId());
        try {
            String inputFilePath = transformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workingDir);
            String outPrefix = geneSampleMetaData.getRunId()
                    + "_" + geneSampleMetaData.getPartIndex()
                    + "_" + geneSampleMetaData.getSubPartIndex();
            String alignedSortedBamPath = samToolsService.sortSam(
                    inputFilePath, workingDir, outPrefix, referenceDatabaseSource.getName());
            FileWrapper fileWrapperToOutput = transformIoHandler.handleFileOutput(gcsService, alignedSortedBamPath);
            fileUtils.deleteDir(workingDir);

            c.output(KV.of(input.getKey(), KV.of(referenceDatabaseSource, fileWrapperToOutput)));
            successCounter.inc();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            fileUtils.deleteDir(workingDir);
            errorCounter.inc();
        }

    }
}
