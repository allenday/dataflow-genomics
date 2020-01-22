package com.google.allenday.genomics.core.processing.other;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.processing.SamBamManipulationService;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SortFn extends DoFn<KV<KV<SampleMetaData, ReferenceDatabase>, FileWrapper>, KV<KV<SampleMetaData, ReferenceDatabase>, FileWrapper>> {

    private Logger LOG = LoggerFactory.getLogger(SortFn.class);
    private GCSService gcsService;

    private TransformIoHandler transformIoHandler;
    private FileUtils fileUtils;
    private SamBamManipulationService samBamManipulationService;

    public SortFn(TransformIoHandler transformIoHandler, SamBamManipulationService samBamManipulationService, FileUtils fileUtils) {
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

        KV<KV<SampleMetaData, ReferenceDatabase>, FileWrapper> input = c.element();
        ReferenceDatabase referenceDatabase = input.getKey().getValue();
        SampleMetaData geneSampleMetaData = input.getKey().getKey();
        FileWrapper fileWrapper = input.getValue();

        if (geneSampleMetaData == null || fileWrapper == null) {
            LOG.error("Data error");
            LOG.error("geneSampleMetaData: " + geneSampleMetaData);
            LOG.error("fileWrapper: " + fileWrapper);
            return;
        }
        if (fileWrapper.getDataType() == FileWrapper.DataType.EMPTY) {
            c.output(KV.of(input.getKey(), fileWrapper));
        } else {
            try {
                String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(geneSampleMetaData.getRunId());
                try {
                    String inputFilePath = transformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workingDir);
                    String alignedSortedBamPath = samBamManipulationService.sortSam(
                            inputFilePath, workingDir, geneSampleMetaData.getRunId(), referenceDatabase.getDbName());

                    c.output(KV.of(input.getKey(), transformIoHandler.handleFileOutput(gcsService, alignedSortedBamPath)));
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                    e.printStackTrace();
                    c.output(KV.of(input.getKey(), FileWrapper.empty()));
                } finally {
                    fileUtils.deleteDir(workingDir);
                }
            } catch (RuntimeException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            }
        }

    }
}
