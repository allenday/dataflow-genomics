package com.google.allenday.genomics.core.processing.index;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.allenday.genomics.core.processing.SamToolsService;
import com.google.allenday.genomics.core.model.SamRecordsMetadaKey;
import com.google.allenday.genomics.core.processing.merge.MergeFn;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CreateBamIndexFn extends DoFn<KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>>,
        KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(CreateBamIndexFn.class);

    private Counter errorCounter = Metrics.counter(MergeFn.class, "index-error-counter");
    private Counter successCounter = Metrics.counter(MergeFn.class, "index-success-counter");

    private GCSService gcsService;

    private TransformIoHandler transformIoHandler;
    private FileUtils fileUtils;
    private SamToolsService samToolsService;

    public CreateBamIndexFn(TransformIoHandler transformIoHandler, SamToolsService samToolsService, FileUtils fileUtils) {
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

        KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>> input = c.element();
        SraSampleId sraSampleId = input.getKey().getSraSampleId();
        KV<ReferenceDatabaseSource, FileWrapper> dbAndFileWrapper = input.getValue();

        if (sraSampleId == null || dbAndFileWrapper == null) {
            LOG.error("Data error");
            LOG.error("sraSample: " + sraSampleId);
            LOG.error("dbAndfileWrapper: " + dbAndFileWrapper);
            throw new RuntimeException("Broken data");
        }
        ReferenceDatabaseSource referenceDatabaseSource = dbAndFileWrapper.getKey();
        FileWrapper fileWrapper = dbAndFileWrapper.getValue();


        String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(sraSampleId.getValue());
        try {
            String inputFilePath = transformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workingDir);
            String indexBamPath = samToolsService.createIndex(inputFilePath);
            FileWrapper fileWrapperToOutput = transformIoHandler.saveFileToGcsOutput(gcsService, indexBamPath);
            fileUtils.deleteDir(workingDir);
            c.output(KV.of(input.getKey(), KV.of(referenceDatabaseSource, fileWrapperToOutput)));
            successCounter.inc();
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            fileUtils.deleteDir(workingDir);
            errorCounter.inc();
        }
    }
}
