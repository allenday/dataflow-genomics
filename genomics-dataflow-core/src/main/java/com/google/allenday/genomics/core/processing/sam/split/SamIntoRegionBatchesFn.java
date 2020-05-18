package com.google.allenday.genomics.core.processing.sam.split;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.pipeline.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SamRecordsChunkMetadataKey;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.allenday.genomics.core.processing.sam.SamToolsService;
import com.google.cloud.storage.Blob;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class SamIntoRegionBatchesFn extends DoFn<KV<SampleRunMetaData, KV<ReferenceDatabaseSource, FileWrapper>>,
        KV<SamRecordsChunkMetadataKey, KV<ReferenceDatabaseSource, FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(SamIntoRegionBatchesFn.class);
    private GcsService gcsService;

    private TransformIoHandler transformIoHandler;
    private FileUtils fileUtils;
    private BatchSamParser batchSamParser;
    private SamToolsService samToolsService;
    private int batchSize;

    public SamIntoRegionBatchesFn(TransformIoHandler transformIoHandler,
                                  SamToolsService samToolsService,
                                  BatchSamParser batchSamParser,
                                  FileUtils fileUtils,
                                  int batchSize) {
        this.transformIoHandler = transformIoHandler;
        this.fileUtils = fileUtils;
        this.samToolsService = samToolsService;
        this.batchSamParser = batchSamParser;
        this.batchSize = batchSize;
    }

    @Setup
    public void setUp() {
        gcsService = GcsService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of sort with input: %s", c.element().toString()));

        KV<SampleRunMetaData, KV<ReferenceDatabaseSource, FileWrapper>> input = c.element();
        SampleRunMetaData sampleRunMetaData = input.getKey();
        KV<ReferenceDatabaseSource, FileWrapper> kv = input.getValue();
        ReferenceDatabaseSource referenceDatabaseSource = kv.getKey();
        FileWrapper fileWrapper = kv.getValue();

        if (sampleRunMetaData == null || fileWrapper == null) {
            LOG.error("Data error");
            LOG.error("sampleRunMetaData: " + sampleRunMetaData);
            LOG.error("fileWrapper: " + fileWrapper);
            return;
        }

        String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(sampleRunMetaData.getSraSample().getValue());
        try {

            Optional<Blob> referenceIndexBlob = referenceDatabaseSource.getReferenceIndexBlob(gcsService, fileUtils);
            if (referenceIndexBlob.isPresent()) {
                ;
                Map<String, Long> contigAndLength = samToolsService.parseIndexToContigAndLengthMap(
                        gcsService.readBlob(referenceIndexBlob.get().getBlobId())
                );

                String inputFilePath = transformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workingDir);
                String outPrefix = sampleRunMetaData.getRunId()
                        + "_" + sampleRunMetaData.getPartIndex()
                        + "_" + sampleRunMetaData.getSubPartIndex();
                String alignedSortedBamPath = samToolsService.sortSam(
                        inputFilePath, workingDir, outPrefix, referenceDatabaseSource.getName());
                fileUtils.deleteFile(inputFilePath);

                batchSamParser.samRecordsBatchesStreamFromBamFile(alignedSortedBamPath, workingDir,
                        (contig, start, end, batchFileName) -> {
                            try {
                                Long maxLength = contigAndLength.get(contig);
                                end = maxLength != null && end > maxLength ? maxLength : end;
                                SamRecordsChunkMetadataKey.Region region = new SamRecordsChunkMetadataKey.Region(contig, start, end);

                                SamRecordsChunkMetadataKey samRecordsChunkMetadataKey = new SamRecordsChunkMetadataKey(sampleRunMetaData.getSraSample(),
                                        referenceDatabaseSource.getName(), region);
                                String filename = outPrefix + samRecordsChunkMetadataKey.generateFileSuffix() + SamToolsService.SORTED_BAM_FILE_SUFFIX;

                                FileWrapper fw = transformIoHandler.saveFileToGcsOutput(gcsService, batchFileName, filename);
                                fileUtils.deleteFile(batchFileName);
                                c.output(KV.of(samRecordsChunkMetadataKey, KV.of(referenceDatabaseSource, fw)));
                            } catch (IOException e) {
                                LOG.error(e.getMessage());
                            }
                        }, batchSize);
            }
            fileUtils.deleteDir(workingDir);

        } catch (Exception e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            fileUtils.deleteDir(workingDir);
        }
    }

}
