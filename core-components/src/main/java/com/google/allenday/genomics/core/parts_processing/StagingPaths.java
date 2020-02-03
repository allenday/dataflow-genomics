package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
import com.google.allenday.genomics.core.processing.align.AlignService;
import com.google.allenday.genomics.core.processing.dv.DeepVariantService;
import com.google.allenday.genomics.core.processing.sam.SamBamManipulationService;

public class StagingPaths {

    private final static String FILE_NAME_PATTERN = "%s_%s";
    public final static String VCF_TO_BQ_PROCESSED_LIST_FILENAME = "vcf_to_bq_processed.csv";

    private String stagingDir;

    private StagingPaths(String stagingDir) {
        this.stagingDir = stagingDir;
    }

    public static StagingPaths init(String stagingDir) {
        return new StagingPaths(stagingDir);
    }

    public String getAlignedFilePattern() {
        return String.format(GenomicsOptions.ALIGNED_OUTPUT_PATH_PATTERN, stagingDir) + FILE_NAME_PATTERN
                + AlignService.SAM_FILE_PREFIX;
    }

    public String getSortedFilePattern() {
        return String.format(GenomicsOptions.SORTED_OUTPUT_PATH_PATTERN, stagingDir) + FILE_NAME_PATTERN
                + SamBamManipulationService.SORTED_BAM_FILE_SUFFIX;
    }

    public String getMergedFilePattern() {
        return String.format(GenomicsOptions.MERGED_OUTPUT_PATH_PATTERN, stagingDir) + FILE_NAME_PATTERN
                + SamBamManipulationService.MERGE_SORTED_FILE_SUFFIX;
    }

    public String getIndexFilePattern() {
        return String.format(GenomicsOptions.BAM_INDEX_OUTPUT_PATH_PATTERN, stagingDir) + FILE_NAME_PATTERN
                + SamBamManipulationService.BAM_INDEX_SUFFIX;
    }

    public String getVcfFilePattern() {
        return String.format(GenomicsOptions.DEEP_VARIANT_OUTPUT_PATH_PATTERN, stagingDir) + FILE_NAME_PATTERN
                + "/" + FILE_NAME_PATTERN + DeepVariantService.DEEP_VARIANT_RESULT_EXTENSION;
    }

    public String getVcfToBqProcessedListFile() {
        return GenomicsOptions.VCF_TO_BQ_PATH + VCF_TO_BQ_PROCESSED_LIST_FILENAME;
    }
}
