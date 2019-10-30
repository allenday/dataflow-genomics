package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.pipeline.AlignerPipelineOptions;

import java.util.List;

public class AlignerOptions {

    private final String ALIGNED_OUTPUT_PATH_PATTERN = "%s/result_aligned_bam/";
    private final String SORTED_OUTPUT_PATH_PATTERN = "%s/result_sorted_bam/";
    private final String MERGED_OUTPUT_PATH_PATTERN = "%s/result_merged_bam/";
    private final String BAM_INDEX_OUTPUT_PATH_PATTERN = "%s/result_index_bam/";

    private String resultBucket;
    private List<String> geneReferences;
    private String allReferencesDirGcsUri;
    private long memoryOutputLimit;

    private String outputDir;
    private String jobTime;

    public AlignerOptions(String resultBucket, List<String> geneReferences,
                          String allReferencesDirGcsUri, String outputDir, long memoryOutputLimit, String jobTime) {
        this.resultBucket = resultBucket;
        this.geneReferences = geneReferences;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
        this.outputDir = outputDir;
        this.memoryOutputLimit = memoryOutputLimit;
        this.jobTime = jobTime;

        if (this.outputDir.charAt(this.outputDir.length() - 1) != '/') {
            this.outputDir = this.outputDir + '/';
        }
    }

    public static AlignerOptions fromAlignerPipelineOptions(AlignerPipelineOptions alignerPipelineOptions, String jobTime) {
        AlignerOptions alignerOptions = new AlignerOptions(
                alignerPipelineOptions.getResultBucket(),
                alignerPipelineOptions.getReferenceNamesList(),
                alignerPipelineOptions.getAllReferencesDirGcsUri(),
                alignerPipelineOptions.getOutputDir(),
                alignerPipelineOptions.getMemoryOutputLimit(),
                jobTime);
        return alignerOptions;
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


    public String getAlignedOutputDir() {
        return String.format(outputDir + ALIGNED_OUTPUT_PATH_PATTERN, jobTime);
    }

    public String getSortedOutputDir() {
        return String.format(outputDir + SORTED_OUTPUT_PATH_PATTERN, jobTime);
    }

    public String getMergedOutputDir() {
        return String.format(outputDir + MERGED_OUTPUT_PATH_PATTERN, jobTime);
    }

    public String getBamIndexOutputDir() {
        return String.format(outputDir + BAM_INDEX_OUTPUT_PATH_PATTERN, jobTime);
    }
}
