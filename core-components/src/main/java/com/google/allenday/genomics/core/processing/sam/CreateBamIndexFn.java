package com.google.allenday.genomics.core.processing.sam;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.allenday.genomics.core.model.SraSampleIdReferencePair;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CreateBamIndexFn extends DoFn<KV<SraSampleIdReferencePair, FileWrapper>,
        KV<SraSampleIdReferencePair, FileWrapper>> {

    private Logger LOG = LoggerFactory.getLogger(CreateBamIndexFn.class);
    private GCSService gcsService;

    private TransformIoHandler transformIoHandler;
    private FileUtils fileUtils;
    private SamBamManipulationService samBamManipulationService;

    public CreateBamIndexFn(TransformIoHandler transformIoHandler, SamBamManipulationService samBamManipulationService, FileUtils fileUtils) {
        this.transformIoHandler = transformIoHandler;
        this.fileUtils = fileUtils;
        this.samBamManipulationService = samBamManipulationService;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of sort with input: %s", c.element().toString()));

        KV<SraSampleIdReferencePair, FileWrapper> input = c.element();
        SraSampleId sraSampleId = input.getKey().getSraSampleId();
        FileWrapper fileWrapper = input.getValue();

        if (sraSampleId == null || fileWrapper == null) {
            LOG.error("Data error");
            LOG.error("sraSample: " + sraSampleId);
            LOG.error("fileWrapper: " + fileWrapper);
            return;
        }

        String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(sraSampleId.getValue());
        try {
            String inputFilePath = transformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workingDir);
            String indexBamPath = samBamManipulationService.createIndex(inputFilePath);
            FileWrapper fileWrapperToOutput = transformIoHandler.saveFileToGcsOutput(gcsService, indexBamPath);
            fileUtils.deleteDir(workingDir);
            c.output(KV.of(input.getKey(), fileWrapperToOutput));
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            fileUtils.deleteDir(workingDir);
        }
    }
}
