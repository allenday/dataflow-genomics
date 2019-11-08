package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.pipeline.GenomicsPipelineOptions;

import java.util.List;

public class GenomicsOptions {

    private final String ALIGNED_OUTPUT_PATH_PATTERN = "%s/result_aligned_bam/";
    private final String SORTED_OUTPUT_PATH_PATTERN = "%s/result_sorted_bam/";
    private final String MERGED_OUTPUT_PATH_PATTERN = "%s/result_merged_bam/";
    private final String BAM_INDEX_OUTPUT_PATH_PATTERN = "%s/result_merged_bam/";
    private final String DEEP_VARIANT_OUTPUT_PATH_PATTERN = "%s/result_dv/";
    private final String ANOMALY_PATH_PATTERN = "%s/anomaly_examples/";

    private String resultBucket;
    private List<String> geneReferences;
    private String allReferencesDirGcsUri;
    private long memoryOutputLimit;

    private String outputDir;

    public GenomicsOptions(String resultBucket, List<String> geneReferences,
                           String allReferencesDirGcsUri, String outputDir, long memoryOutputLimit) {
        this.resultBucket = resultBucket;
        this.geneReferences = geneReferences;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
        this.outputDir = outputDir;
        this.memoryOutputLimit = memoryOutputLimit;

        if (this.outputDir.charAt(this.outputDir.length() - 1) != '/') {
            this.outputDir = this.outputDir + '/';
        }
    }

    public static GenomicsOptions fromAlignerPipelineOptions(GenomicsPipelineOptions alignerPipelineOptions) {
        GenomicsOptions genomicsOptions = new GenomicsOptions(
                alignerPipelineOptions.getResultBucket(),
                alignerPipelineOptions.getReferenceNamesList(),
                alignerPipelineOptions.getAllReferencesDirGcsUri(),
                alignerPipelineOptions.getOutputDir(),
                alignerPipelineOptions.getMemoryOutputLimit());
        return genomicsOptions;
    }

    public String getResultBucket() {
        return resultBucket;
    }

    public List<String> getGeneReferences() {
        return geneReferences;
    }

    public String getAllReferencesDirGcsUri() {
        return allReferencesDirGcsUri;
    }

    public long getMemoryOutputLimit() {
        return memoryOutputLimit;
    }


    public String getAlignedOutputDirPattern() {
        return outputDir + ALIGNED_OUTPUT_PATH_PATTERN;
    }

    public String getSortedOutputDirPattern() {
        return outputDir + SORTED_OUTPUT_PATH_PATTERN;
    }

    public String getMergedOutputDirPattern() {
        return outputDir + MERGED_OUTPUT_PATH_PATTERN;
    }

    public String getBamIndexOutputDirPattern() {
        return outputDir + BAM_INDEX_OUTPUT_PATH_PATTERN;
    }

    public String getAnomalyOutputDirPattern() {
        return outputDir + ANOMALY_PATH_PATTERN;
    }

    public String getDeepVariantOutputDirPattern() {
        return outputDir + DEEP_VARIANT_OUTPUT_PATH_PATTERN;
    }

    public String getCustomOutputDirPattern(String patternSuffix) {
        return outputDir + patternSuffix;
    }
}
