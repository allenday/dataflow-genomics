package com.google.allenday.genomics.core.pipeline;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

import java.util.List;

public interface GenomicsPipelineOptions extends DataflowPipelineOptions {

    @Description("Name of GCS bucket with all resuts data")
    @Validation.Required
    String getResultBucket();

    void setResultBucket(String value);

    @Description("Fasta model reference names list")
    @Validation.Required
    List<String> getReferenceNamesList();

    void setReferenceNamesList(List<String> value);

    @Description("GCS dir path with references")
    @Validation.Required
    String getAllReferencesDirGcsUri();

    void setAllReferencesDirGcsUri(String value);

    @Description("GCS dir path for processing output")
    @Validation.Required
    String getOutputDir();

    void setOutputDir(String value);

    @Description("Threshold to decide how to pass data between transforms")
    @Default.Long(0)
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

    Integer getShards();

    void setShards(Integer shards);
}
