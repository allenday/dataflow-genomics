package com.google.allenday.genomics.core.align.transform;

import com.google.allenday.genomics.core.align.SamBamManipulationService;
import com.google.allenday.genomics.core.gene.FileWrapper;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.gene.ReferenceDatabase;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SortFn extends DoFn<KV<KV<GeneExampleMetaData, ReferenceDatabase>, FileWrapper>, KV<KV<GeneExampleMetaData, ReferenceDatabase>, FileWrapper>> {

    private Logger LOG = LoggerFactory.getLogger(SortFn.class);
    private GCSService gcsService;

    private TransformIoHandler transformIoHandler;
    private FileUtils fileUtils;
    private SamBamManipulationService samBamManipulationService;

    public SortFn(TransformIoHandler transformIoHandler, FileUtils fileUtils, SamBamManipulationService samBamManipulationService) {
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

        KV<KV<GeneExampleMetaData, ReferenceDatabase>, FileWrapper> input = c.element();
        ReferenceDatabase referenceDatabase = input.getKey().getValue();
        GeneExampleMetaData geneExampleMetaData = input.getKey().getKey();
        FileWrapper fileWrapper = input.getValue();

        if (geneExampleMetaData == null || fileWrapper == null) {
            LOG.error("Data error");
            LOG.error("geneExampleMetaData: " + geneExampleMetaData);
            LOG.error("fileWrapper: " + fileWrapper);
            return;
        }
        try {
            String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(geneExampleMetaData.getRunId());
            try {
                String inputFilePath = transformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workingDir);
                String alignedSortedBamPath = samBamManipulationService.sortSam(
                        inputFilePath, workingDir, geneExampleMetaData.getRunId(), referenceDatabase.getDbName());

                c.output(KV.of(input.getKey(), transformIoHandler.handleFileOutput(gcsService, alignedSortedBamPath)));
            } catch (IOException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            } finally {
                fileUtils.deleteDir(workingDir);
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }

    }
}
