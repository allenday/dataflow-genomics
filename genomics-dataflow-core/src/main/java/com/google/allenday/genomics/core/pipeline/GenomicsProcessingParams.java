package com.google.allenday.genomics.core.pipeline;

import com.google.allenday.genomics.core.model.Aligner;
import com.google.allenday.genomics.core.model.VariantCaller;
import org.apache.beam.sdk.options.ValueProvider;
import org.javatuples.Pair;

import java.util.List;

public class GenomicsProcessingParams {

    public final static String INTERMEDIATE_PREFIX = "%s/intermediate/";
    public final static String FINAL_PREFIX = "%s/final/";

    public final static String CHUNKS_BY_COUNT_OUTPUT_PATH_PATTERN = "chunked_fastq_by_count/";
    public final static String CHUNKS_BY_SIZE_OUTPUT_PATH_PATTERN = "chunked_fastq_by_size/";
    public final static String ALIGNED_OUTPUT_PATH_PATTERN = "aligned_sam/";
    public final static String SORTED_OUTPUT_PATH_PATTERN = "sorted_bam/";
    public final static String SORTED_AND_SPLITTED_PATH_PATTERN = "sorted_and_splitted_bam/";
    public final static String FINAL_MERGED_PATH_PATTERN = "result_merged_bam/";
    public final static String MERGED_REGIONS_PATH_PATTERN = "merged_regions_bam/";
    public final static String VARIANT_CALLING_OUTPUT_PATH_PATTERN = "result_variant_calling/";
    public final static String VCF_TO_BQ_PATH = "vcf_to_bq/";
    public final static String ANOMALY_PATH_PATTERN = "anomaly_samples/";

    private Aligner aligner;
    private String resultBucket;
    private List<String> geneReferences;
    private String allReferencesDirGcsUri;
    private ValueProvider<String> refDataJsonString;
    private long memoryOutputLimit;
    private DeepVariantOptions deepVariantOptions;
    private VariantCaller variantCaller;

    private String vcfBqDatasetAndTablePattern;
    private String outputDir;

    public GenomicsProcessingParams(Aligner aligner, String resultBucket, List<String> geneReferences,
                                    String allReferencesDirGcsUri, ValueProvider<String> refDataJsonString,
                                    VariantCaller variantCaller,
                                    String outputDir,
                                    long memoryOutputLimit) {
        this.aligner = aligner;
        this.resultBucket = resultBucket;
        this.geneReferences = geneReferences;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
        this.refDataJsonString = refDataJsonString;
        this.variantCaller = variantCaller;
        this.outputDir = outputDir;
        this.memoryOutputLimit = memoryOutputLimit;

        if (this.outputDir.charAt(this.outputDir.length() - 1) != '/') {
            this.outputDir = this.outputDir + '/';
        }
    }

    public static GenomicsProcessingParams fromAlignerPipelineOptions(GenomicsPipelineOptions alignerPipelineOptions) {

        Pair<String, String> bucketDirPair = splitGcsPath(alignerPipelineOptions.getOutputGcsUri());
        GenomicsProcessingParams genomicsProcessingParams = new GenomicsProcessingParams(
                alignerPipelineOptions.getAligner(),
                bucketDirPair.getValue0(),
                alignerPipelineOptions.getReferenceNamesList(),
                alignerPipelineOptions.getAllReferencesDirGcsUri(),
                alignerPipelineOptions.getRefDataJsonString(),
                alignerPipelineOptions.getVariantCaller(),
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
        genomicsProcessingParams.setDeepVariantOptions(deepVariantOptions);
        genomicsProcessingParams.setVcfBqDatasetAndTablePattern(alignerPipelineOptions.getVcfBqDatasetAndTablePattern());
        return genomicsProcessingParams;
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
        return outputDir + INTERMEDIATE_PREFIX + CHUNKS_BY_COUNT_OUTPUT_PATH_PATTERN;
    }

    public String getChuncksBySizeOutputDirPattern() {
        return outputDir + INTERMEDIATE_PREFIX + CHUNKS_BY_SIZE_OUTPUT_PATH_PATTERN;
    }

    public String getAlignedOutputDirPattern() {
        return outputDir + INTERMEDIATE_PREFIX + ALIGNED_OUTPUT_PATH_PATTERN;
    }

    public String getSortedOutputDirPattern() {
        return outputDir + INTERMEDIATE_PREFIX + SORTED_OUTPUT_PATH_PATTERN;
    }

    public String getSortedAndSplittedOutputDirPattern() {
        return outputDir + INTERMEDIATE_PREFIX + SORTED_AND_SPLITTED_PATH_PATTERN;
    }

    public String getMergedRegionsDirPattern() {
        return outputDir + INTERMEDIATE_PREFIX + MERGED_REGIONS_PATH_PATTERN;
    }

    public String getFinalMergedDirPattern() {
        return outputDir + FINAL_PREFIX + FINAL_MERGED_PATH_PATTERN;
    }

    public String getAnomalyOutputDirPattern() {
        return outputDir + FINAL_PREFIX + ANOMALY_PATH_PATTERN;
    }

    public String getVariantCallingOutputDirPattern() {
        return outputDir + FINAL_PREFIX + VARIANT_CALLING_OUTPUT_PATH_PATTERN;
    }

    public String getVcfToBqOutputDir() {
        return outputDir + INTERMEDIATE_PREFIX + VCF_TO_BQ_PATH;
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

    public Aligner getAligner() {
        return aligner;
    }

    public VariantCaller getVariantCaller() {
        return variantCaller;
    }
}
