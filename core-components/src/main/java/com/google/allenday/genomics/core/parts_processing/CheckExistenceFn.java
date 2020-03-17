package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.IoUtils;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CheckExistenceFn extends DoFn<KV<SraSampleId, Iterable<KV<SampleMetaData, List<FileWrapper>>>>, String> {

    private Logger LOG = LoggerFactory.getLogger(CheckExistenceFn.class);

    private FileUtils fileUtils;
    private IoUtils ioUtils;
    private ValueProvider<List<String>> referencesVP;
    private GCSService gcsService;
    private DecimalFormat decimalFormat = new DecimalFormat("#.###");

    private StagingPathsBulder stagingPathsBulder;

    public CheckExistenceFn(FileUtils fileUtils, IoUtils ioUtils, ValueProvider<List<String>> referencesVP,
                            StagingPathsBulder stagingPathsBulder) {
        this.fileUtils = fileUtils;
        this.ioUtils = ioUtils;
        this.referencesVP = referencesVP;
        this.stagingPathsBulder = stagingPathsBulder;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        KV<SraSampleId, Iterable<KV<SampleMetaData, List<FileWrapper>>>> input = c.element();
        SraSampleId sraSampleId = input.getKey();

        LOG.info(String.format("Work with %s", sraSampleId.getValue()));
        List<String> outputElements = new ArrayList<>();
        outputElements.add(sraSampleId.getValue());

        boolean fastqSumCounted = false;
        long sumOfFastq = 0;

        List<String> references = referencesVP.get();
        for (String ref : references) {
            String processedVcfToBq = "";
            try {
                processedVcfToBq = gcsService.readBlob(stagingPathsBulder.getVcfToBqProcessedListFileBlobId(), ioUtils);
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

            for (KV<SampleMetaData, List<FileWrapper>> geneSampleMetaDataAndUris : input.getValue()) {
                SampleMetaData geneSampleMetaData = geneSampleMetaDataAndUris.getKey();
                BlobId blobIdAlign = stagingPathsBulder.buildAlignedBlobId(geneSampleMetaData.getRunId(), ref);
                BlobId blobIdSort = stagingPathsBulder.buildSortedBlobId(geneSampleMetaData.getRunId(), ref);
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
