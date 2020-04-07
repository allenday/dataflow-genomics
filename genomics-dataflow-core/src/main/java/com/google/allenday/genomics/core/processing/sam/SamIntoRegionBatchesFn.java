package com.google.allenday.genomics.core.processing.sam;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.IoUtils;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.cloud.storage.Blob;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class SamIntoRegionBatchesFn extends DoFn<KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>,
        KV<SamRecordsMetadaKey, KV<ReferenceDatabaseSource, FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(SamIntoRegionBatchesFn.class);
    private GCSService gcsService;

    private TransformIoHandler transformIoHandler;
    private FileUtils fileUtils;
    private IoUtils ioUtils;
    private BatchSamParser batchSamParser;
    private SamBamManipulationService samBamManipulationService;
    private int batchSize;

    public SamIntoRegionBatchesFn(TransformIoHandler transformIoHandler,
                                  SamBamManipulationService samBamManipulationService,
                                  BatchSamParser batchSamParser,
                                  FileUtils fileUtils,
                                  IoUtils ioUtils,
                                  int batchSize) {
        this.transformIoHandler = transformIoHandler;
        this.fileUtils = fileUtils;
        this.ioUtils = ioUtils;
        this.samBamManipulationService = samBamManipulationService;
        this.batchSamParser = batchSamParser;
        this.batchSize = batchSize;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of sort with input: %s", c.element().toString()));

        KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>> input = c.element();
        SampleMetaData sampleMetaData = input.getKey();
        KV<ReferenceDatabaseSource, FileWrapper> kv = input.getValue();
        ReferenceDatabaseSource referenceDatabaseSource = kv.getKey();
        FileWrapper fileWrapper = kv.getValue();

        if (sampleMetaData == null || fileWrapper == null) {
            LOG.error("Data error");
            LOG.error("sampleMetaData: " + sampleMetaData);
            LOG.error("fileWrapper: " + fileWrapper);
            return;
        }

        try {
            String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(sampleMetaData.getSraSample().getValue());
            try {

                Optional<Blob> referenceIndexBlob = referenceDatabaseSource.getReferenceIndexBlob(gcsService, fileUtils);
                if (referenceIndexBlob.isPresent()) {
                    ;
                    Map<String, Long> contigAndLength = samBamManipulationService.parseIndexToContigAndLengthMap(
                            gcsService.readBlob(referenceIndexBlob.get().getBlobId(), ioUtils)
                    );

                    String inputFilePath = transformIoHandler.handleInputAsLocalFile(gcsService, fileWrapper, workingDir);
                    String outPrefix = sampleMetaData.getRunId()
                            + "_" + sampleMetaData.getPartIndex()
                            + "_" + sampleMetaData.getSubPartIndex();
                    String alignedSortedBamPath = samBamManipulationService.sortSam(
                            inputFilePath, workingDir, outPrefix, referenceDatabaseSource.getName());
                    fileUtils.deleteFile(inputFilePath);

                    batchSamParser.samRecordsBatchesStreamFromBamFile(alignedSortedBamPath, workingDir,
                            (contig, start, end, batchFileName) -> {
                                try {
                                    Long maxLength = contigAndLength.get(contig);
                                    end = maxLength != null && end > maxLength ? maxLength : end;
                                    SamRecordsMetadaKey.Region region = new SamRecordsMetadaKey.Region(contig, start, end);

                                    SamRecordsMetadaKey samRecordsMetadaKey = new SamRecordsMetadaKey(sampleMetaData.getSraSample(),
                                            referenceDatabaseSource.getName(), region);
                                    String filename = outPrefix + samRecordsMetadaKey.generateFileSuffix() + SamBamManipulationService.SORTED_BAM_FILE_SUFFIX;

                                    FileWrapper fw = transformIoHandler.saveFileToGcsOutput(gcsService, batchFileName, filename);
                                    fileUtils.deleteFile(batchFileName);
                                    c.output(KV.of(samRecordsMetadaKey, KV.of(referenceDatabaseSource, fw)));
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
        } catch (RuntimeException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }

    }

}
