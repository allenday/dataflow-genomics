package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.processing.sam.SamBamManipulationService;
import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.allenday.genomics.core.reference.ReferenceProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class AlignAndSortFn extends DoFn<KV<SampleMetaData, KV<List<ReferenceDatabaseSource>, List<FileWrapper>>>,
        KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(AlignAndSortFn.class);
    private GCSService gcsService;

    private AlignService alignService;
    private ReferenceProvider referencesProvider;
    private TransformIoHandler alignTransformIoHandler;
    private TransformIoHandler sortTransformIoHandler;
    private FileUtils fileUtils;
    private SamBamManipulationService samBamManipulationService;

    public AlignAndSortFn(AlignService alignService,
                          SamBamManipulationService samBamManipulationService,
                          ReferenceProvider referencesProvider,
                          TransformIoHandler alignTransformIoHandler,
                          TransformIoHandler sortTransformIoHandler,
                          FileUtils fileUtils) {
        this.alignService = alignService;
        this.referencesProvider = referencesProvider;
        this.alignTransformIoHandler = alignTransformIoHandler;
        this.sortTransformIoHandler = sortTransformIoHandler;
        this.fileUtils = fileUtils;
        this.samBamManipulationService = samBamManipulationService;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
        alignService.setup();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of processing with input: %s", c.element().toString()));

        SampleMetaData geneSampleMetaData = c.element().getKey();

        KV<List<ReferenceDatabaseSource>, List<FileWrapper>> fnValue = c.element().getValue();
        List<ReferenceDatabaseSource> referenceDBSources = fnValue.getKey();

        List<FileWrapper> fileWrapperList = fnValue.getValue();

        if (geneSampleMetaData == null || fileWrapperList.size() == 0 || referenceDBSources == null || referenceDBSources.size() == 0) {
            LOG.error("Data error");
            LOG.error("geneSampleMetaData: " + geneSampleMetaData);
            LOG.error("fileWrapperList.size(): " + fileWrapperList.size());
            LOG.error("referenceDBSource: " + referenceDBSources);
            return;
        }
        try {
            String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(geneSampleMetaData.getRunId());
            try {
                List<String> srcFilesPaths = fileWrapperList.stream()
                        .map(geneData -> alignTransformIoHandler.handleInputAsLocalFile(gcsService, geneData, workingDir))
                        .collect(Collectors.toList());
                for (ReferenceDatabaseSource referenceDBSource : referenceDBSources) {
                    ReferenceDatabase referenceDatabase =
                            referencesProvider.getReferenceDbWithDownload(gcsService, referenceDBSource);
                    String alignedSamPath = null;
                    String alignedSortedBamPath = null;
                    try {
                        String outPrefix = geneSampleMetaData.getRunId()
                                + "_" + geneSampleMetaData.getPartIndex()
                                + "_" + geneSampleMetaData.getSubPartIndex();
                        alignedSamPath = alignService.alignFastq(
                                referenceDatabase.getFastaLocalPath(),
                                srcFilesPaths,
                                workingDir, outPrefix,
                                referenceDatabase.getDbName(),
                                geneSampleMetaData.getSraSample().getValue(),
                                geneSampleMetaData.getPlatform());
                        alignedSortedBamPath = samBamManipulationService.sortSam(
                                alignedSamPath, workingDir, outPrefix, referenceDBSource.getName());
                        alignTransformIoHandler.handleFileOutput(gcsService, alignedSamPath);
                        FileWrapper sortFileWrapper = sortTransformIoHandler.handleFileOutput(gcsService, alignedSortedBamPath);
                        fileUtils.deleteFile(alignedSamPath);

                        c.output(KV.of(geneSampleMetaData, KV.of(referenceDBSource, sortFileWrapper)));
                    } catch (IOException e) {
                        LOG.error(e.getMessage());
                        e.printStackTrace();
                        if (alignedSamPath != null) {
                            fileUtils.deleteFile(alignedSamPath);
                        }
                        if (alignedSortedBamPath != null) {
                            fileUtils.deleteFile(alignedSortedBamPath);
                        }

                        c.output(KV.of(geneSampleMetaData, KV.of(referenceDBSource, FileWrapper.empty())));
                    }
                }
            } catch (Exception e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
                fileUtils.deleteDir(workingDir);
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }


}
