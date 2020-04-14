package com.google.allenday.genomics.core.processing.merge;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SamRecordsMetadaKey;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.allenday.genomics.core.processing.SamToolsService;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MergeFn extends DoFn<KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, List<FileWrapper>>>,
        KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>>> {


    private Logger LOG = LoggerFactory.getLogger(MergeFn.class);

    private Counter errorCounter = Metrics.counter(MergeFn.class, "merge-error-counter");
    private Counter successCounter = Metrics.counter(MergeFn.class, "merge-success-counter");

    private GCSService gcsService;

    private TransformIoHandler transformIoHandler;
    private SamToolsService samToolsService;
    private FileUtils fileUtils;

    public MergeFn(TransformIoHandler transformIoHandler, SamToolsService samToolsService, FileUtils fileUtils) {
        this.transformIoHandler = transformIoHandler;
        this.samToolsService = samToolsService;
        this.fileUtils = fileUtils;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    private boolean isNothingToMerge(List fileWrapperList) {
        return fileWrapperList.size() < 2;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Merge of sort with input: %s", c.element().toString()));

        SamRecordsMetadaKey samRecordsMetadaKey = c.element().getKey();

        KV<ReferenceDatabaseSource, List<FileWrapper>> fnValue = c.element().getValue();
        ReferenceDatabaseSource referenceDatabaseSource = fnValue.getKey();
        List<FileWrapper> fileWrappers = fnValue.getValue();

        if (samRecordsMetadaKey == null || fileWrappers.size() == 0) {
            LOG.error("Data error");
            LOG.error("samRecordsMetadaKey: " + samRecordsMetadaKey);
            LOG.error("fileWrappers.size(): " + fileWrappers.size());
            throw new RuntimeException("Broken data");
        }
        SraSampleId sraSampleId = samRecordsMetadaKey.getSraSampleId();

        String workDir = fileUtils.makeDirByCurrentTimestampAndSuffix(sraSampleId.getValue());

        try {
            if (isNothingToMerge(fileWrappers)) {
                fileWrappers.stream().findFirst().ifPresent(fileWrapper -> {
                            String mergedFilename =
                                    samToolsService.generateMergedFileName(sraSampleId.getValue(),
                                            samRecordsMetadaKey.generateFileSuffix());
                            FileWrapper fileWrapperMerged = transformIoHandler.handleInputAndCopyToGcs(
                                    fileWrapper, gcsService, mergedFilename, workDir);
                            fileUtils.deleteDir(workDir);

                            c.output(KV.of(samRecordsMetadaKey, KV.of(referenceDatabaseSource, fileWrapperMerged)));
                            successCounter.inc();
                        }
                );
            } else {
                List<String> localBamPaths = fileWrappers.stream()
                        .map(geneData -> transformIoHandler.handleInputAsLocalFile(gcsService, geneData, workDir))
                        .collect(Collectors.toList());

                String mergedFileName = samToolsService.mergeBamFiles(localBamPaths, workDir,
                        sraSampleId.getValue(), samRecordsMetadaKey.generateFileSuffix());
                FileWrapper fileWrapperMerged = transformIoHandler.saveFileToGcsOutput(gcsService, mergedFileName);
                fileUtils.deleteDir(workDir);

                c.output(KV.of(samRecordsMetadaKey, KV.of(referenceDatabaseSource, fileWrapperMerged)));
                successCounter.inc();
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            fileUtils.deleteDir(workDir);
            errorCounter.inc();
        }
    }
}
