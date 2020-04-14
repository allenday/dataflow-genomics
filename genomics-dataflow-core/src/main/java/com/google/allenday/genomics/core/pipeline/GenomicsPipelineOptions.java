package com.google.allenday.genomics.core.pipeline;

import com.google.allenday.genomics.core.model.Aligner;
import com.google.allenday.genomics.core.model.VariantCaller;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import java.util.List;

public interface GenomicsPipelineOptions extends DataflowPipelineOptions {

    @Description("Sequence aligner. Supports: MINIMAP2, BWA")
    @Default.Enum("MINIMAP2")
    Aligner getAligner();

    void setAligner(Aligner value);

    @Description("Fasta model reference names list")
    List<String> getReferenceNamesList();

    void setReferenceNamesList(List<String> value);

    @Description("GCS dir path with references")
    String getAllReferencesDirGcsUri();

    void setAllReferencesDirGcsUri(String value);


    @Description("GCS dir path with references")
    ValueProvider<String> getRefDataJsonString();

    void setRefDataJsonString(ValueProvider<String> value);

    @Description("GCS dir path for processing output")
    @Validation.Required
    String getOutputGcsUri();

    void setOutputGcsUri(String value);

    @Description("SRA samples to filter")
    List<String> getSraSamplesToFilter();

    void setSraSamplesToFilter(List<String> value);

    @Description("SRA samples to skip")
    List<String> getSraSamplesToSkip();

    void setSraSamplesToSkip(List<String> value);

    @Description("Threshold to decide how to pass data between transforms")
    @Default.Long(800)
    long getMemoryOutputLimit();

    void setMemoryOutputLimit(long value);

    String getControlPipelineWorkerRegion();

    void setControlPipelineWorkerRegion(String controlPipelineWorkerRegion);

    String getStepsWorkerRegion();

    void setStepsWorkerRegion(String stepsWorkerRegion);

    Integer getMakeExamplesCoresPerWorker();

    void setMakeExamplesCoresPerWorker(Integer makeExamplesCoresPerWorker);

    Integer getMakeExamplesRamPerWorker();

    void setMakeExamplesRamPerWorker(Integer makeExamplesRamPerWorker);

    Integer getMakeExamplesDiskPerWorker();

    void setMakeExamplesDiskPerWorker(Integer makeExamplesDiskPerWorker);

    Integer getCallVariantsCoresPerWorker();

    void setCallVariantsCoresPerWorker(Integer callVariantsCoresPerWorker);

    Integer getCallVariantsRamPerWorker();

    void setCallVariantsRamPerWorker(Integer callVariantsRamPerWorker);

    Integer getCallVariantsDiskPerWorker();

    void setCallVariantsDiskPerWorker(Integer callVariantsDiskPerWorker);

    Integer getPostprocessVariantsCores();

    void setPostprocessVariantsCores(Integer postprocessVariantsCores);

    Integer getPostprocessVariantsRam();

    void setPostprocessVariantsRam(Integer postprocessVariantsRam);

    Integer getPostprocessVariantsDisk();

    void setPostprocessVariantsDisk(Integer postprocessVariantsDisk);

    Integer getMakeExamplesWorkers();

    void setMakeExamplesWorkers(Integer makeExamplesWorkers);

    Integer getCallVariantsWorkers();

    void setCallVariantsWorkers(Integer callVariantsWorkers);

    Boolean getPreemptible();

    void setPreemptible(Boolean preemptible);

    Integer getMaxPremptibleTries();

    void setMaxPremptibleTries(Integer maxPremptibleTries);

    Integer getMaxNonPremptibleTries();

    void setMaxNonPremptibleTries(Integer maxNonPremptibleTries);

    Integer getDeepVariantShards();

    void setDeepVariantShards(Integer shards);

    @Description("BigQuery dataset and table name pattern for storing VCF results")
    String getVcfBqDatasetAndTablePattern();

    void setVcfBqDatasetAndTablePattern(String value);

    @Description("Name of Variant Caller service. Supports: GATK, DEEP_VARIANT")
    @Default.Enum("GATK")
    VariantCaller getVariantCaller();

    void setVariantCaller(VariantCaller value);
}
