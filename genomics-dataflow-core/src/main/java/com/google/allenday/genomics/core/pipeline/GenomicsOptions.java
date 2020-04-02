package com.google.allenday.genomics.core.pipeline;

import org.apache.beam.sdk.options.ValueProvider;
import org.javatuples.Pair;

import java.util.List;

public class GenomicsOptions {

    public final static String CHUNKS_BY_COUNT_OUTPUT_PATH_PATTERN = "%s/result_chunked_fastq_by_count/";
    public final static String CHUNKS_BY_SIZE_OUTPUT_PATH_PATTERN = "%s/result_chunked_fastq_by_size/";
    public final static String ALIGNED_OUTPUT_PATH_PATTERN = "%s/result_aligned_bam/";
    public final static String SORTED_OUTPUT_PATH_PATTERN = "%s/result_sorted_bam/";
    public final static String MERGED_OUTPUT_PATH_PATTERN = "%s/result_merged_bam/";
    public final static String BAM_INDEX_OUTPUT_PATH_PATTERN = "%s/result_merged_bam/";
    public final static String DEEP_VARIANT_OUTPUT_PATH_PATTERN = "%s/result_dv/";
    public final static String VCF_TO_BQ_PATH = "/vcf_to_bq/";
    public final static String ANOMALY_PATH_PATTERN = "%s/anomaly_samples/";

    private String aligner;
    private String resultBucket;
    private List<String> geneReferences;
    private String allReferencesDirGcsUri;
    private ValueProvider<String> refDataJsonString;
    private long memoryOutputLimit;
    private DeepVariantOptions deepVariantOptions;

    private String vcfBqDatasetAndTablePattern;
    private String outputDir;

    public GenomicsOptions(String aligner, String resultBucket, List<String> geneReferences,
                           String allReferencesDirGcsUri, ValueProvider<String> refDataJsonString,
                           String outputDir,
                           long memoryOutputLimit) {
        this.aligner = aligner;
        this.resultBucket = resultBucket;
        this.geneReferences = geneReferences;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
        this.refDataJsonString = refDataJsonString;
        this.outputDir = outputDir;
        this.memoryOutputLimit = memoryOutputLimit;

        if (this.outputDir.charAt(this.outputDir.length() - 1) != '/') {
            this.outputDir = this.outputDir + '/';
        }
    }

    public static GenomicsOptions fromAlignerPipelineOptions(GenomicsPipelineOptions alignerPipelineOptions) {

        Pair<String, String> bucketDirPair = splitGcsPath(alignerPipelineOptions.getOutputGcsUri());
        GenomicsOptions genomicsOptions = new GenomicsOptions(
                alignerPipelineOptions.getAligner(),
                bucketDirPair.getValue0(),
                alignerPipelineOptions.getReferenceNamesList(),
                alignerPipelineOptions.getAllReferencesDirGcsUri(),
                alignerPipelineOptions.getRefDataJsonString(),
                bucketDirPair.getValue1(),
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

    private static Pair<String, String> splitGcsPath(String uri) {
        String workPart = uri.split("//")[1];
        String[] parts = workPart.split("/");
        String bucket = parts[0];
        String name = workPart.replace(bucket + "/", "");
        return Pair.with(bucket, name);
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

    public String getChuncksByCountOutputDirPattern() {
        return outputDir + CHUNKS_BY_COUNT_OUTPUT_PATH_PATTERN;
    }

    public String getChuncksBySizeOutputDirPattern() {
        return outputDir + CHUNKS_BY_SIZE_OUTPUT_PATH_PATTERN;
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

    public String getBaseOutputDir() {
        return outputDir;
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

    public ValueProvider<String> getRefDataJsonString() {
        return refDataJsonString;
    }

    public String getAligner() {
        return aligner;
    }
}
