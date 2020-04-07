package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
import com.google.allenday.genomics.core.processing.align.AlignService;
import com.google.allenday.genomics.core.processing.sam.SamBamManipulationService;
import com.google.allenday.genomics.core.processing.variantcall.DeepVariantService;
import com.google.cloud.storage.BlobId;

import java.io.Serializable;

public class StagingPathsBulder implements Serializable {

    private final static String FILE_NAME_PATTERN = "%s_%s";
    private final static String VCF_TO_BQ_PROCESSED_LIST_FILENAME = "vcf_to_bq_processed.csv";
    private final static String EXISTENCE_FILE_NAME = "existence.csv";

    private String stagingBucket;
    private String stagingDir;

    private StagingPathsBulder(String stagingBucket, String stagingDir) {
        this.stagingBucket = stagingBucket;
        this.stagingDir = stagingDir;
    }

    public static StagingPathsBulder init(String stagingBucket, String stagingDir) {
        return new StagingPathsBulder(stagingBucket, stagingDir);
    }

    public String getStagingBucket() {
        return stagingBucket;
    }

    public String getStagingDir() {
        return stagingDir;
    }

    BlobId buildAlignedBlobId(String runId, String reference) {
        String pathPattern = String.format(GenomicsOptions.ALIGNED_OUTPUT_PATH_PATTERN, stagingDir) + FILE_NAME_PATTERN
                + AlignService.SAM_FILE_PREFIX;
        return BlobId.of(stagingBucket, String.format(pathPattern, runId, reference));
    }

    BlobId buildSortedBlobId(String runId, String reference) {
        String pathPattern = String.format(GenomicsOptions.SORTED_OUTPUT_PATH_PATTERN, stagingDir) + FILE_NAME_PATTERN
                + SamBamManipulationService.SORTED_BAM_FILE_SUFFIX;
        return BlobId.of(stagingBucket, String.format(pathPattern, runId, reference));
    }

    BlobId buildMergedBlobId(String sraSample, String reference) {
        String pathPattern = String.format(GenomicsOptions.FINAL_MERGED_PATH_PATTERN, stagingDir) + FILE_NAME_PATTERN
                + SamBamManipulationService.MERGE_SORTED_FILE_SUFFIX;
        return BlobId.of(stagingBucket, String.format(pathPattern, sraSample, reference));
    }

    BlobId buildIndexBlobId(String sraSample, String reference) {
        String pathPattern = String.format(GenomicsOptions.FINAL_MERGED_PATH_PATTERN, stagingDir) + FILE_NAME_PATTERN
                + SamBamManipulationService.MERGE_SORTED_FILE_SUFFIX + SamBamManipulationService.BAM_INDEX_SUFFIX;
        return BlobId.of(stagingBucket, String.format(pathPattern, sraSample, reference));

    }

    public String buildVcfDirPath() {
        return String.format(GenomicsOptions.VARIANT_CALLING_OUTPUT_PATH_PATTERN, stagingDir);
    }

    BlobId buildVcfFileBlobId(String sraSample, String reference) {
        String pathPattern = buildVcfDirPath() + FILE_NAME_PATTERN + DeepVariantService.DEEP_VARIANT_RESULT_EXTENSION;
        return BlobId.of(stagingBucket, String.format(pathPattern, sraSample, reference));
    }

    public BlobId getVcfToBqProcessedListFileBlobId() {
        return BlobId.of(stagingBucket, stagingDir +
                GenomicsOptions.VCF_TO_BQ_PATH.replace("%s", "") +
                VCF_TO_BQ_PROCESSED_LIST_FILENAME);
    }

    public String getExistenceCsvUri() {
        return String.format("gs://%s/%s", stagingBucket, stagingDir + "/" + EXISTENCE_FILE_NAME);
    }
}
