package com.google.allenday.genomics.core.align;

import com.google.allenday.genomics.core.pipeline.AlignerPipelineOptions;

import java.util.List;

public class AlignerOptions {

    private String srcBucket;
    private String resultBucket;
    private List<String> geneReferences;
    private String allReferencesDirGcsUri;
    private String alignedOutputDir;
    private long memoryOutputLimit;

    private String sortedOutputDir;
    private String mergedOutputDir;

    public AlignerOptions(String srcBucket, String resultBucket, List<String> geneReferences,
                          String allReferencesDirGcsUri, String alignedOutputDir, long memoryOutputLimit) {
        this.srcBucket = srcBucket;
        this.resultBucket = resultBucket;
        this.geneReferences = geneReferences;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
        this.alignedOutputDir = alignedOutputDir;
        this.memoryOutputLimit = memoryOutputLimit;
    }

    public static AlignerOptions fromAlignerPipelineOptions(AlignerPipelineOptions alignerPipelineOptions){
        AlignerOptions alignerOptions = new AlignerOptions(
                alignerPipelineOptions.getSrcBucket(),
                alignerPipelineOptions.getResultBucket(),
                alignerPipelineOptions.getReferenceNamesList(),
                alignerPipelineOptions.getAllReferencesDirGcsUri(),
                alignerPipelineOptions.getAlignedOutputDir(),
                alignerPipelineOptions.getMemoryOutputLimit());
        alignerOptions.setMergedOutputDir(alignerPipelineOptions.getMergedOutputDir());
        alignerOptions.setSortedOutputDir(alignerPipelineOptions.getSortedOutputDir());
        return alignerOptions;
    }

    public void setSortedOutputDir(String sortedOutputDir) {
        this.sortedOutputDir = sortedOutputDir;
    }

    public void setMergedOutputDir(String mergedOutputDir) {
        this.mergedOutputDir = mergedOutputDir;
    }

    public String getSrcBucket() {
        return srcBucket;
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

    public String getAlignedOutputDir() {
        return alignedOutputDir;
    }

    public long getMemoryOutputLimit() {
        return memoryOutputLimit;
    }

    public String getSortedOutputDir() {
        return sortedOutputDir;
    }

    public String getMergedOutputDir() {
        return mergedOutputDir;
    }
}
