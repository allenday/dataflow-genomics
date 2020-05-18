package com.google.allenday.genomics.core.pipeline.batch.partsprocessing;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CheckExistenceFn extends DoFn<KV<SraSampleId, Iterable<KV<SampleRunMetaData, List<FileWrapper>>>>, String> {

    private Logger LOG = LoggerFactory.getLogger(CheckExistenceFn.class);

    private FileUtils fileUtils;
    private List<String> references;
    private GcsService gcsService;
    private DecimalFormat decimalFormat = new DecimalFormat("#.###");

    private StagingPathsBulder stagingPathsBulder;

    public CheckExistenceFn(FileUtils fileUtils, List<String> references,
                            StagingPathsBulder stagingPathsBulder) {
        this.fileUtils = fileUtils;
        this.references = references;
        this.stagingPathsBulder = stagingPathsBulder;
    }

    @Setup
    public void setUp() {
        gcsService = GcsService.initialize(fileUtils);
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        KV<SraSampleId, Iterable<KV<SampleRunMetaData, List<FileWrapper>>>> input = c.element();
        SraSampleId sraSampleId = input.getKey();

        LOG.info(String.format("Work with %s", sraSampleId.getValue()));
        List<String> outputElements = new ArrayList<>();
        outputElements.add(sraSampleId.getValue());

        boolean fastqSumCounted = false;
        long sumOfFastq = 0;
        for (String ref : references) {
            String processedVcfToBq = "";
            try {
                processedVcfToBq = gcsService.readBlob(stagingPathsBulder.getVcfToBqProcessedListFileBlobId());
            } catch (Exception ignored) {
            }
            boolean existsVcfToBq = processedVcfToBq.contains(sraSampleId + "," + ref);
            if (existsVcfToBq) {
                outputElements.add("7_SAVED_TO_BQ");
                continue;
            }
            BlobId blobIdDv = stagingPathsBulder.buildVcfFileBlobId(sraSampleId.getValue(), ref);
            boolean existsDv = gcsService.isExists(blobIdDv);
            if (existsDv) {
                outputElements.add("6_Vcf_to_Bq");
                continue;
            }

            BlobId blobIdIndex = stagingPathsBulder.buildIndexBlobId(sraSampleId.getValue(), ref);
            boolean existsIndex = gcsService.isExists(blobIdIndex);
            if (existsIndex) {
                outputElements.add("5_DV");
                continue;
            }

            BlobId blobIdMerge = stagingPathsBulder.buildMergedBlobId(sraSampleId.getValue(), ref);
            boolean existsMerge = gcsService.isExists(blobIdMerge);
            if (existsMerge) {
                outputElements.add("4_Index");
                continue;
            }

            int alignExistenceCounter = 0;
            int sortExistenceCounter = 0;

            outputElements.add(String.valueOf(StreamSupport.stream(input.getValue().spliterator(), false).count()));

            for (KV<SampleRunMetaData, List<FileWrapper>> geneSampleMetaDataAndUris : input.getValue()) {
                SampleRunMetaData geneSampleRunMetaData = geneSampleMetaDataAndUris.getKey();
                BlobId blobIdAlign = stagingPathsBulder.buildAlignedBlobId(geneSampleRunMetaData.getRunId(), ref);
                BlobId blobIdSort = stagingPathsBulder.buildSortedBlobId(geneSampleRunMetaData.getRunId(), ref);
                boolean existsAlign = gcsService.isExists(blobIdAlign);
                boolean existsSort = gcsService.isExists(blobIdSort);

                if (!fastqSumCounted) {
                    long filesSizeSum = geneSampleMetaDataAndUris.getValue().stream()
                            .map(fileWrapper -> gcsService.getBlobSize(gcsService.getBlobIdFromUri(fileWrapper.getBlobUri())))
                            .collect(Collectors.summarizingLong(Long::longValue)).getSum();
                    sumOfFastq = sumOfFastq + filesSizeSum;
                }
                if (existsAlign) {
                    alignExistenceCounter++;
                }
                if (existsSort) {
                    sortExistenceCounter++;
                }
            }
            fastqSumCounted = true;
            long readGroupSize = StreamSupport.stream(input.getValue().spliterator(), false).count();

            if (sortExistenceCounter == readGroupSize) {
                outputElements.add("3_Merge");
                continue;
            }
            if (alignExistenceCounter == readGroupSize) {
                outputElements.add(String.format("2_Sort (%d/%d)", sortExistenceCounter, readGroupSize));
            } else {
                outputElements.add(String.format("1_Align (%d/%d)", sortExistenceCounter, readGroupSize));
            }
        }
        outputElements.add(decimalFormat.format(sumOfFastq / (float) (1024 * 1024 * 1024)));
        BlobId blobIdMerge = stagingPathsBulder.buildMergedBlobId(sraSampleId.getValue(), references.get(0));

        boolean existsMerge = gcsService.isExists(blobIdMerge);
        float mergedSortedBamSizeMb = existsMerge ? gcsService.getBlobSize(blobIdMerge) / (float) (1024 * 1024 * 1024) : 0;
        outputElements.add(decimalFormat.format(mergedSortedBamSizeMb));
        c.output(String.join(",", outputElements));
    }
}
