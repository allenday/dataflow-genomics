package com.google.allenday.genomics.core.processing.sam;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.allenday.genomics.core.model.SraSampleIdReferencePair;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MergeFn extends DoFn<KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, List<FileWrapper>>>,
        KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, FileWrapper>>> {


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

    private boolean isNothingToMerge(List fileWrapperList) {
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

        SraSampleIdReferencePair sraSampleIdReferencePair = c.element().getKey();

        if (sraSampleIdReferencePair == null) {
            LOG.error("Data error");
            LOG.error("sraSampleIdReferencePair: " + sraSampleIdReferencePair);
            return;
        }
        SraSampleId sraSampleId = sraSampleIdReferencePair.getSraSampleId();

        KV<ReferenceDatabaseSource, List<FileWrapper>> fnValue = c.element().getValue();
        ReferenceDatabaseSource referenceDatabaseSource = fnValue.getKey();
        List<FileWrapper> fileWrappers = fnValue.getValue();

        if (sraSampleId == null || fileWrappers.size() == 0) {
            LOG.error("Data error");
            LOG.error("sraSampleId: " + sraSampleId);
            LOG.error("fileWrappers.size(): " + fileWrappers.size());
            return;
        }
        if (containesEmpty(fileWrappers)) {
            LOG.info(String.format("Group %s contains empty FileWrappers", sraSampleId));
            return;
        }
        try {
            String workDir = fileUtils.makeDirByCurrentTimestampAndSuffix(sraSampleId.getValue());
            try {
                if (isNothingToMerge(fileWrappers)) {
                    fileWrappers.stream().findFirst().ifPresent(fileWrapper -> {
                                String mergedFilename =
                                        samBamManipulationService.generateMergedFileName(sraSampleId.getValue(),
                                                referenceDatabaseSource.getName());
                                FileWrapper fileWrapperMerged = transformIoHandler.handleInputAndCopyToGcs(
                                        fileWrapper, gcsService, mergedFilename, workDir);
                                fileUtils.deleteDir(workDir);

                                c.output(KV.of(sraSampleIdReferencePair, KV.of(referenceDatabaseSource, fileWrapperMerged)));

                            }
                    );
                } else {
                    List<String> localBamPaths = fileWrappers.stream()
                            .map(geneData -> transformIoHandler.handleInputAsLocalFile(gcsService, geneData, workDir))
                            .collect(Collectors.toList());

                    String mergedFileName = samBamManipulationService.mergeBamFiles(localBamPaths, workDir,
                            sraSampleId.getValue(), referenceDatabaseSource.getName());
                    FileWrapper fileWrapperMerged = transformIoHandler.saveFileToGcsOutput(gcsService, mergedFileName);
                    fileUtils.deleteDir(workDir);

                    c.output(KV.of(sraSampleIdReferencePair, KV.of(referenceDatabaseSource, fileWrapperMerged)));

                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
                fileUtils.deleteDir(workDir);
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
