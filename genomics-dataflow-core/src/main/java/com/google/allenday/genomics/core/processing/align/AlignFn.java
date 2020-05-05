package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.pipeline.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.allenday.genomics.core.reference.ReferenceProvider;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AlignFn extends DoFn<KV<SampleRunMetaData, KV<List<ReferenceDatabaseSource>, List<FileWrapper>>>,
        KV<SampleRunMetaData, KV<ReferenceDatabaseSource, FileWrapper>>> {


    private Counter errorCounter = Metrics.counter(AlignFn.class, "align-error-counter");
    private Counter successCounter = Metrics.counter(AlignFn.class, "align-success-counter");

    private Logger LOG = LoggerFactory.getLogger(AlignFn.class);
    private GcsService gcsService;

    private AlignService alignService;
    private ReferenceProvider referencesProvider;
    private TransformIoHandler transformIoHandler;
    private FileUtils fileUtils;

    public AlignFn(AlignService alignService,
                   ReferenceProvider referencesProvider,
                   TransformIoHandler transformIoHandler,
                   FileUtils fileUtils) {
        this.alignService = alignService;
        this.referencesProvider = referencesProvider;
        this.transformIoHandler = transformIoHandler;
        this.fileUtils = fileUtils;
    }

    @Setup
    public void setUp() {
        gcsService = GcsService.initialize(fileUtils);
        alignService.setup();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        LOG.info(String.format("Start of processing with input: %s", c.element().toString()));

        SampleRunMetaData geneSampleRunMetaData = c.element().getKey();

        KV<List<ReferenceDatabaseSource>, List<FileWrapper>> fnValue = c.element().getValue();
        List<ReferenceDatabaseSource> referenceDBSources = fnValue.getKey();

        List<FileWrapper> fileWrapperList = fnValue.getValue();

        if (geneSampleRunMetaData == null || fileWrapperList.size() == 0 || referenceDBSources == null || referenceDBSources.size() == 0) {
            LOG.error("Data error");
            LOG.error("geneSampleRunMetaData: " + geneSampleRunMetaData);
            LOG.error("fileWrapperList.size(): " + fileWrapperList.size());
            LOG.error("referenceDBSource: " + referenceDBSources);
            throw new RuntimeException("Broken data");
        }

        String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(geneSampleRunMetaData.getRunId());

        try {
            List<String> srcFilesPaths = new ArrayList<>();
            for (FileWrapper fileWrapper : fileWrapperList) {
                srcFilesPaths.add(transformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workingDir));
            }
            for (ReferenceDatabaseSource referenceDBSource : referenceDBSources) {
                ReferenceDatabase referenceDatabase =
                        referencesProvider.getReferenceDbWithDownload(gcsService, referenceDBSource);
                String alignedSamPath = null;
                try {
                    String outPrefix = geneSampleRunMetaData.getRunId()
                            + "_" + geneSampleRunMetaData.getPartIndex()
                            + "_" + geneSampleRunMetaData.getSubPartIndex();
                    alignedSamPath = alignService.alignFastq(
                            referenceDatabase.getFastaLocalPath(),
                            srcFilesPaths,
                            workingDir, outPrefix,
                            referenceDatabase.getDbName(),
                            geneSampleRunMetaData.getSraSample().getValue(),
                            geneSampleRunMetaData.getPlatform());
                    FileWrapper fileWrapper = transformIoHandler.handleFileOutput(gcsService, alignedSamPath);
                    fileUtils.deleteFile(alignedSamPath);

                    successCounter.inc();
                    c.output(KV.of(geneSampleRunMetaData, KV.of(referenceDBSource, fileWrapper)));
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                    e.printStackTrace();
                    if (alignedSamPath != null) {
                        fileUtils.deleteFile(alignedSamPath);
                    }
                    errorCounter.inc();
                }
            }
            fileUtils.deleteDir(workingDir);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            fileUtils.deleteDir(workingDir);
            errorCounter.inc(fileWrapperList.size());
        }
    }
}
