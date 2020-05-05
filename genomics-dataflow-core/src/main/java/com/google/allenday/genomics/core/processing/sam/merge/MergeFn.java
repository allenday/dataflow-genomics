package com.google.allenday.genomics.core.processing.sam.merge;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.pipeline.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SamRecordsChunkMetadataKey;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.allenday.genomics.core.processing.sam.SamToolsService;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MergeFn extends DoFn<KV<SamRecordsChunkMetadataKey, KV<ReferenceDatabaseSource, List<FileWrapper>>>,
        KV<SamRecordsChunkMetadataKey, KV<ReferenceDatabaseSource, FileWrapper>>> {


    private Logger LOG = LoggerFactory.getLogger(MergeFn.class);

    private Counter errorCounter = Metrics.counter(MergeFn.class, "merge-error-counter");
    private Counter successCounter = Metrics.counter(MergeFn.class, "merge-success-counter");

    private GcsService gcsService;

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
        gcsService = GcsService.initialize(fileUtils);
    }

    private boolean isNothingToMerge(List fileWrapperList) {
        return fileWrapperList.size() < 2;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Merge of sort with input: %s", c.element().toString()));

        SamRecordsChunkMetadataKey samRecordsChunkMetadataKey = c.element().getKey();

        KV<ReferenceDatabaseSource, List<FileWrapper>> fnValue = c.element().getValue();
        ReferenceDatabaseSource referenceDatabaseSource = fnValue.getKey();
        List<FileWrapper> fileWrappers = fnValue.getValue();

        if (samRecordsChunkMetadataKey == null || fileWrappers.size() == 0) {
            LOG.error("Data error");
            LOG.error("samRecordsChunkMetadataKey: " + samRecordsChunkMetadataKey);
            LOG.error("fileWrappers.size(): " + fileWrappers.size());
            throw new RuntimeException("Broken data");
        }
        SraSampleId sraSampleId = samRecordsChunkMetadataKey.getSraSampleId();

        String workDir = fileUtils.makeDirByCurrentTimestampAndSuffix(sraSampleId.getValue());

        try {
            if (isNothingToMerge(fileWrappers)) {
                Optional<FileWrapper> firstFileWrapper = fileWrappers.stream().findFirst();
                if (firstFileWrapper.isPresent()) {
                    String mergedFilename =
                            samToolsService.generateMergedFileName(sraSampleId.getValue(),
                                    samRecordsChunkMetadataKey.generateFileSuffix());
                    FileWrapper fileWrapperMerged = transformIoHandler.handleInputAndCopyToGcs(
                            firstFileWrapper.get(), gcsService, mergedFilename, workDir);
                    fileUtils.deleteDir(workDir);

                    c.output(KV.of(samRecordsChunkMetadataKey, KV.of(referenceDatabaseSource, fileWrapperMerged)));
                    successCounter.inc();
                }
            } else {
                List<String> localBamPaths = new ArrayList<>();
                for (FileWrapper fileWrapper : fileWrappers) {
                    localBamPaths.add(transformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workDir));
                }

                String mergedFileName = samToolsService.mergeBamFiles(localBamPaths, workDir,
                        sraSampleId.getValue(), samRecordsChunkMetadataKey.generateFileSuffix());
                FileWrapper fileWrapperMerged = transformIoHandler.saveFileToGcsOutput(gcsService, mergedFileName);
                fileUtils.deleteDir(workDir);

                c.output(KV.of(samRecordsChunkMetadataKey, KV.of(referenceDatabaseSource, fileWrapperMerged)));
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
