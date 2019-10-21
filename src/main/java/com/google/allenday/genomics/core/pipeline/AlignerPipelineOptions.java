package com.google.allenday.genomics.core.pipeline;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

import java.util.List;

public interface AlignerPipelineOptions extends DataflowPipelineOptions {

    @Description("Name of GCS bucket with all source data")
    @Validation.Required
    String getSrcBucket();

    void setSrcBucket(String value);

    @Description("Name of GCS bucket with all resuts data")
    @Validation.Required
    String getResultBucket();

    void setResultBucket(String value);

    @Description("Fasta gene reference names list")
    @Validation.Required
    List<String> getReferenceNamesList();

    void setReferenceNamesList(List<String> value);

    @Description("GCS dir path with references")
    String getAllReferencesDirGcsUri();

    void setAllReferencesDirGcsUri(String value);

    @Description("GCS dir path for align output")
    String getAlignedOutputDir();

    void setAlignedOutputDir(String value);

    @Description("Threshold to decide how to pass data between anomaly")
    long getMemoryOutputLimit();

    void setMemoryOutputLimit(long value);

    @Description("GCS dir path for sor output")
    String getSortedOutputDir();

    void setSortedOutputDir(String value);

    @Description("GCS dir path for merge output")
    String getMergedOutputDir();

    void setMergedOutputDir(String value);
}
