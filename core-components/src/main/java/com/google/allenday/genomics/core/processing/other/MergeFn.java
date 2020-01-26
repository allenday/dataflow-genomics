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
import java.util.List;
import java.util.stream.Collectors;

public class MergeFn extends DoFn<KV<KV<ReadGroupMetaData, ReferenceDatabase>, List<FileWrapper>>, KV<KV<ReadGroupMetaData, ReferenceDatabase>, FileWrapper>> {


    private Logger LOG = LoggerFactory.getLogger(MergeFn.class);
    private GCSService gcsService;

    private TransformIoHandler transformIoHandler;
    private SamBamManipulationService samBamManipulationService;
    private FileUtils fileUtils;

    public MergeFn(TransformIoHandler transformIoHandler, SamBamManipulationService samBamManipulationService, FileUtils fileUtils) {
        this.transformIoHandler = transformIoHandler;
        this.samBamManipulationService = samBamManipulationService;
        this.fileUtils = fileUtils;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    private boolean isNothingToMerge(List<FileWrapper> fileWrapperList) {
        return fileWrapperList.size() < 2;
    }


    private boolean containesEmpty(List<FileWrapper> fileWrapperList) {
        return fileWrapperList.stream().anyMatch(fileWrapper -> {
            assert fileWrapper.getDataType() != null;
            return fileWrapper.getDataType().equals(FileWrapper.DataType.EMPTY);
        });
    }


    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Merge of sort with input: %s", c.element().toString()));

        KV<ReadGroupMetaData, ReferenceDatabase> geneReadGroupMetaDataAndReference = c.element().getKey();

        if (geneReadGroupMetaDataAndReference == null) {
            LOG.error("Data error");
            LOG.error("geneReadGroupMetaDataAndReference: " + geneReadGroupMetaDataAndReference);
            return;
        }
        ReadGroupMetaData geneReadGroupMetaData = geneReadGroupMetaDataAndReference.getKey();
        ReferenceDatabase referenceDatabase = geneReadGroupMetaDataAndReference.getValue();

        List<FileWrapper> fileWrapperList = c.element().getValue();

        if (geneReadGroupMetaData == null || fileWrapperList.size() == 0) {
            LOG.error("Data error");
            LOG.error("geneReadGroupMetaData: " + geneReadGroupMetaDataAndReference);
            LOG.error("fileWrapperList.size(): " + fileWrapperList.size());
            return;
        }
        if (containesEmpty(fileWrapperList)){
            LOG.info(String.format("Group %s contains empty FileWrappers", geneReadGroupMetaData.toString()));
            return;
        }
        try {
            String workDir = fileUtils.makeDirByCurrentTimestampAndSuffix(geneReadGroupMetaData.getSraSample());
            try {
                if (isNothingToMerge(fileWrapperList)) {
                    fileWrapperList.stream().findFirst().ifPresent(inputGeneData -> {
                                String mergedFilename =
                                        samBamManipulationService.generateMergedFileName(geneReadGroupMetaData.getSraSample(), referenceDatabase.getDbName());
                                FileWrapper fileWrapper = transformIoHandler.handleInputAndCopyToGcs(
                                        inputGeneData,
                                        gcsService,
                                        mergedFilename,
                                        workDir);
                                c.output(KV.of(geneReadGroupMetaDataAndReference, fileWrapper));
                            }
                    );
                } else {
                    List<String> localBamPaths = fileWrapperList.stream()
                            .map(geneData -> transformIoHandler.handleInputAsLocalFile(gcsService, geneData, workDir))
                            .collect(Collectors.toList());

                    String mergedFileName = samBamManipulationService.mergeBamFiles(localBamPaths, workDir,
                            geneReadGroupMetaData.getSraSample(), referenceDatabase.getDbName());
                    FileWrapper fileWrapper = transformIoHandler.saveFileToGcsOutput(gcsService, mergedFileName);
                    c.output(KV.of(geneReadGroupMetaDataAndReference, fileWrapper));

                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            } finally {
                fileUtils.deleteDir(workDir);
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
