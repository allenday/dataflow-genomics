package com.google.allenday.genomics.core.pipeline;

import java.io.Serializable;
import java.util.Optional;

public class DeepVariantOptions implements Serializable {

    private final static String DEFAULT_DEEP_VARIANT_CONTROL_PIPELINE_WORKER_REGION = "us-central1";
    private final static String DEFAULT_DEEP_VARIANT_STEPS_WORKER_REGION =
            DEFAULT_DEEP_VARIANT_CONTROL_PIPELINE_WORKER_REGION;

    private String controlPipelineWorkerRegion;
    private String stepsWorkerRegion;
    private Integer makeExamplesCoresPerWorker;
    private Integer makeExamplesRamPerWorker;
    private Integer makeExamplesDiskPerWorker;
    private Integer callVariantsCoresPerWorker;
    private Integer callVariantsRamPerWorker;
    private Integer callVariantsDiskPerWorker;
    private Integer postprocessVariantsCores;
    private Integer postprocessVariantsRam;
    private Integer postprocessVariantsDisk;
    private Integer makeExamplesWorkers;
    private Integer callVariantsWorkers;
    private Boolean preemptible;
    private Integer maxPremptibleTries;
    private Integer maxNonPremptibleTries;
    private Integer shards;

    public DeepVariantOptions() {
    }

    public String getControlPipelineWorkerRegion() {
        return Optional.ofNullable(controlPipelineWorkerRegion).orElse(DEFAULT_DEEP_VARIANT_CONTROL_PIPELINE_WORKER_REGION);
    }

    public void setControlPipelineWorkerRegion(String controlPipelineWorkerRegion) {
        this.controlPipelineWorkerRegion = controlPipelineWorkerRegion;
    }

    public String getStepsWorkerRegion() {
        return Optional.ofNullable(stepsWorkerRegion).orElse(DEFAULT_DEEP_VARIANT_STEPS_WORKER_REGION);
    }

    public void setStepsWorkerRegion(String stepsWorkerRegion) {
        this.stepsWorkerRegion = stepsWorkerRegion;
    }

    public Optional<Integer> getMakeExamplesCoresPerWorker() {
        return Optional.ofNullable(makeExamplesCoresPerWorker);
    }

    public void setMakeExamplesCoresPerWorker(Integer makeExamplesCoresPerWorker) {
        this.makeExamplesCoresPerWorker = makeExamplesCoresPerWorker;
    }

    public Optional<Integer> getMakeExamplesRamPerWorker() {
        return Optional.ofNullable(makeExamplesRamPerWorker);
    }

    public void setMakeExamplesRamPerWorker(Integer makeExamplesRamPerWorker) {
        this.makeExamplesRamPerWorker = makeExamplesRamPerWorker;
    }

    public Optional<Integer> getMakeExamplesDiskPerWorker() {
        return Optional.ofNullable(makeExamplesDiskPerWorker);
    }

    public void setMakeExamplesDiskPerWorker(Integer makeExamplesDiskPerWorker) {
        this.makeExamplesDiskPerWorker = makeExamplesDiskPerWorker;
    }

    public Optional<Integer> getCallVariantsCoresPerWorker() {
        return Optional.ofNullable(callVariantsCoresPerWorker);
    }

    public void setCallVariantsCoresPerWorker(Integer callVariantsCoresPerWorker) {
        this.callVariantsCoresPerWorker = callVariantsCoresPerWorker;
    }

    public Optional<Integer> getCallVariantsRamPerWorker() {
        return Optional.ofNullable(callVariantsRamPerWorker);
    }

    public void setCallVariantsRamPerWorker(Integer callVariantsRamPerWorker) {
        this.callVariantsRamPerWorker = callVariantsRamPerWorker;
    }

    public Optional<Integer> getCallVariantsDiskPerWorker() {
        return Optional.ofNullable(callVariantsDiskPerWorker);
    }

    public void setCallVariantsDiskPerWorker(Integer callVariantsDiskPerWorker) {
        this.callVariantsDiskPerWorker = callVariantsDiskPerWorker;
    }

    public Optional<Integer> getPostprocessVariantsCores() {
        return Optional.ofNullable(postprocessVariantsCores);
    }

    public void setPostprocessVariantsCores(Integer postprocessVariantsCores) {
        this.postprocessVariantsCores = postprocessVariantsCores;
    }

    public Optional<Integer> getPostprocessVariantsRam() {
        return Optional.ofNullable(postprocessVariantsRam);
    }

    public void setPostprocessVariantsRam(Integer postprocessVariantsRam) {
        this.postprocessVariantsRam = postprocessVariantsRam;
    }

    public Optional<Integer> getPostprocessVariantsDisk() {
        return Optional.ofNullable(postprocessVariantsDisk);
    }

    public void setPostprocessVariantsDisk(Integer postprocessVariantsDisk) {
        this.postprocessVariantsDisk = postprocessVariantsDisk;
    }

    public Optional<Integer> getMakeExamplesWorkers() {
        return Optional.ofNullable(makeExamplesWorkers);
    }

    public void setMakeExamplesWorkers(Integer makeExamplesWorkers) {
        this.makeExamplesWorkers = makeExamplesWorkers;
    }

    public Optional<Integer> getCallVariantsWorkers() {
        return Optional.ofNullable(callVariantsWorkers);
    }

    public void setCallVariantsWorkers(Integer callVariantsWorkers) {
        this.callVariantsWorkers = callVariantsWorkers;
    }

    public Optional<Boolean> getPreemptible() {
        return Optional.ofNullable(preemptible);
    }

    public void setPreemptible(Boolean preemptible) {
        this.preemptible = preemptible;
    }

    public Optional<Integer> getMaxPremptibleTries() {
        return Optional.ofNullable(maxPremptibleTries);
    }

    public void setMaxPremptibleTries(Integer maxPremptibleTries) {
        this.maxPremptibleTries = maxPremptibleTries;
    }

    public Optional<Integer> getMaxNonPremptibleTries() {
        return Optional.ofNullable(maxNonPremptibleTries);
    }

    public void setMaxNonPremptibleTries(Integer maxNonPremptibleTries) {
        this.maxNonPremptibleTries = maxNonPremptibleTries;
    }

    public Optional<Integer> getShards() {
        return Optional.ofNullable(shards);
    }

    public void setShards(Integer shards) {
        this.shards = shards;
    }
}
