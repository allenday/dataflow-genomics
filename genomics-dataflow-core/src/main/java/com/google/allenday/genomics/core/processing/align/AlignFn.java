package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
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
import java.util.List;
import java.util.stream.Collectors;

public class AlignFn extends DoFn<KV<SampleMetaData, KV<List<ReferenceDatabaseSource>, List<FileWrapper>>>,
        KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>> {


    private Counter errorCounter = Metrics.counter(AlignFn.class, "align-error-counter");
    private Counter successCounter = Metrics.counter(AlignFn.class, "align-success-counter");

    private Logger LOG = LoggerFactory.getLogger(AlignFn.class);
    private GCSService gcsService;

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
            throw new RuntimeException("Broken data");
        }

        String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(geneSampleMetaData.getRunId());

        try {
            List<String> srcFilesPaths = fileWrapperList.stream()
                    .map(geneData -> transformIoHandler.handleInputAsLocalFile(gcsService, geneData, workingDir))
                    .collect(Collectors.toList());
            for (ReferenceDatabaseSource referenceDBSource : referenceDBSources) {
                ReferenceDatabase referenceDatabase =
                        referencesProvider.getReferenceDbWithDownload(gcsService, referenceDBSource);
                String alignedSamPath = null;
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
                    FileWrapper fileWrapper = transformIoHandler.handleFileOutput(gcsService, alignedSamPath);
                    fileUtils.deleteFile(alignedSamPath);

                    successCounter.inc();
                    c.output(KV.of(geneSampleMetaData, KV.of(referenceDBSource, fileWrapper)));
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
        } catch (RuntimeException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            fileUtils.deleteDir(workingDir);
            errorCounter.inc(fileWrapperList.size());
        }
    }
}
