package com.google.allenday.genomics.core.processing.other;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.ReadGroupMetaData;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.processing.SamBamManipulationService;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CreateBamIndexFn extends DoFn<KV<KV<ReadGroupMetaData, ReferenceDatabase>, FileWrapper>,
        KV<KV<ReadGroupMetaData, ReferenceDatabase>, FileWrapper>> {

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

        KV<KV<ReadGroupMetaData, ReferenceDatabase>, FileWrapper> input = c.element();
        ReadGroupMetaData geneReadGroupMetaData = input.getKey().getKey();
        FileWrapper fileWrapper = input.getValue();

        if (geneReadGroupMetaData == null || fileWrapper == null) {
            LOG.error("Data error");
            LOG.error("geneReadGroupMetaData: " + geneReadGroupMetaData);
            LOG.error("fileWrapper: " + fileWrapper);
            return;
        }
        String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(geneReadGroupMetaData.getSraSample());
        try {
            String inputFilePath = transformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workingDir);
            String indexBamPath = samBamManipulationService.createIndex(
                    inputFilePath);

            c.output(KV.of(input.getKey(), transformIoHandler.saveFileToGcsOutput(gcsService, indexBamPath)));
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        } finally {
            fileUtils.deleteDir(workingDir);
        }
    }
}
