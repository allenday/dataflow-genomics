package com.google.allenday.genomics.core.pipeline;

import java.util.List;

public class GenomicsOptions {

    private final String ALIGNED_OUTPUT_PATH_PATTERN = "%s/result_aligned_bam/";
    private final String SORTED_OUTPUT_PATH_PATTERN = "%s/result_sorted_bam/";
    private final String MERGED_OUTPUT_PATH_PATTERN = "%s/result_merged_bam/";
    private final String BAM_INDEX_OUTPUT_PATH_PATTERN = "%s/result_merged_bam/";
    private final String DEEP_VARIANT_OUTPUT_PATH_PATTERN = "%s/result_dv/";
    private final String VCF_TO_BQ_PATH = "vcf_to_bq/";
    private final String ANOMALY_PATH_PATTERN = "%s/anomaly_examples/";

    private String resultBucket;
    private List<String> geneReferences;
    private String allReferencesDirGcsUri;
    private long memoryOutputLimit;
    private DeepVariantOptions deepVariantOptions;

    private String vcfBqDatasetAndTablePattern;
    private String outputDir;

    public GenomicsOptions(String resultBucket, List<String> geneReferences,
                           String allReferencesDirGcsUri, String outputDir,
                           long memoryOutputLimit) {
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

        DeepVariantOptions deepVariantOptions = new DeepVariantOptions();
        deepVariantOptions.setControlPipelineWorkerRegion(alignerPipelineOptions.getControlPipelineWorkerRegion());
        deepVariantOptions.setStepsWorkerRegion(alignerPipelineOptions.getStepsWorkerRegion());

        deepVariantOptions.setMakeExamplesCoresPerWorker(alignerPipelineOptions.getMakeExamplesCoresPerWorker());
        deepVariantOptions.setMakeExamplesRamPerWorker(alignerPipelineOptions.getMakeExamplesRamPerWorker());
        deepVariantOptions.setMakeExamplesDiskPerWorker(alignerPipelineOptions.getMakeExamplesDiskPerWorker());

        deepVariantOptions.setCallVariantsCoresPerWorker(alignerPipelineOptions.getCallVariantsCoresPerWorker());
        deepVariantOptions.setCallVariantsRamPerWorker(alignerPipelineOptions.getCallVariantsRamPerWorker());
        deepVariantOptions.setCallVariantsDiskPerWorker(alignerPipelineOptions.getCallVariantsDiskPerWorker());

        deepVariantOptions.setPostprocessVariantsCores(alignerPipelineOptions.getPostprocessVariantsCores());
        deepVariantOptions.setPostprocessVariantsRam(alignerPipelineOptions.getPostprocessVariantsRam());
        deepVariantOptions.setPostprocessVariantsDisk(alignerPipelineOptions.getPostprocessVariantsDisk());

        deepVariantOptions.setMakeExamplesWorkers(alignerPipelineOptions.getMakeExamplesWorkers());
        deepVariantOptions.setCallVariantsWorkers(alignerPipelineOptions.getCallVariantsWorkers());
        deepVariantOptions.setPreemptible(alignerPipelineOptions.getPreemptible());
        deepVariantOptions.setMaxPremptibleTries(alignerPipelineOptions.getMaxNonPremptibleTries());
        deepVariantOptions.setMaxNonPremptibleTries(alignerPipelineOptions.getMaxNonPremptibleTries());
        deepVariantOptions.setDeepVariantShards(alignerPipelineOptions.getDeepVariantShards());
        genomicsOptions.setDeepVariantOptions(deepVariantOptions);
        genomicsOptions.setVcfBqDatasetAndTablePattern(alignerPipelineOptions.getVcfBqDatasetAndTablePattern());
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

    public String getVcfToBqOutputDir() {
        return outputDir + VCF_TO_BQ_PATH;
    }

    public String getCustomOutputDirPattern(String patternSuffix) {
        return outputDir + patternSuffix;
    }

    public DeepVariantOptions getDeepVariantOptions() {
        return deepVariantOptions;
    }

    public void setDeepVariantOptions(DeepVariantOptions deepVariantOptions) {
        this.deepVariantOptions = deepVariantOptions;
    }

    public void setVcfBqDatasetAndTablePattern(String vcfBqDatasetAndTablePattern) {
        this.vcfBqDatasetAndTablePattern = vcfBqDatasetAndTablePattern;
    }

    public String getVcfBqDatasetAndTablePattern() {
        return vcfBqDatasetAndTablePattern;
    }
}
