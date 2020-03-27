package com.google.allenday.genomics.core.processing.sam;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.BamWithIndexUris;
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

public class MergeAndIndexFn extends DoFn<KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, List<FileWrapper>>>,
        KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, BamWithIndexUris>>> {


    private Logger LOG = LoggerFactory.getLogger(MergeAndIndexFn.class);
    private GCSService gcsService;

    private TransformIoHandler mergeTransformIoHandler;
    private TransformIoHandler indexTransformIoHandler;
    private SamBamManipulationService samBamManipulationService;
    private FileUtils fileUtils;

    public MergeAndIndexFn(TransformIoHandler mergeTransformIoHandler, TransformIoHandler indexTransformIoHandler,
                           SamBamManipulationService samBamManipulationService, FileUtils fileUtils) {
        this.mergeTransformIoHandler = mergeTransformIoHandler;
        this.indexTransformIoHandler = indexTransformIoHandler;
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
                String mergedFileName;
                FileWrapper fileWrapperMerged;
                if (isNothingToMerge(fileWrappers)) {
                    if (fileWrappers.size() > 0 && fileWrappers.get(0) != null) {
                        FileWrapper fileWrapper = fileWrappers.get(0);

                        String srcBam = mergeTransformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workDir);
                        mergedFileName = samBamManipulationService.generateMergedFileName(sraSampleId.getValue(),
                                referenceDatabaseSource.getName());
                        fileWrapperMerged = mergeTransformIoHandler.saveFileToGcsOutput(gcsService, srcBam,
                                fileUtils.getFilenameFromPath(mergedFileName));
                    } else {
                        return;
                    }
                } else {
                    List<String> localBamPaths = fileWrappers.stream()
                            .map(geneData -> mergeTransformIoHandler.handleInputAsLocalFile(gcsService, geneData, workDir))
                            .collect(Collectors.toList());

                    mergedFileName = samBamManipulationService.mergeBamFiles(localBamPaths, workDir,
                            sraSampleId.getValue(), referenceDatabaseSource.getName());


                    fileWrapperMerged = mergeTransformIoHandler.saveFileToGcsOutput(gcsService, mergedFileName);
                }
                String indexBamPath = samBamManipulationService.createIndex(mergedFileName);

                FileWrapper fileWrapperIndex = indexTransformIoHandler.saveFileToGcsOutput(gcsService, indexBamPath);
                fileUtils.deleteDir(workDir);

                c.output(KV.of(sraSampleIdReferencePair, KV.of(referenceDatabaseSource,
                        new BamWithIndexUris(fileWrapperMerged.getBlobUri(), fileWrapperIndex.getBlobUri()))));
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
