package com.google.allenday.genomics.core.batch;

import com.google.allenday.genomics.core.pipeline.GenomicsPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface BatchProcessingPipelineOptions extends GenomicsPipelineOptions {

    @Description("Name of GCS bucket with all source data")
    @Validation.Required
    String getSrcBucket();

    void setSrcBucket(String value);

    @Description("GCS uri pattern of CSV files with input data")
    @Validation.Required
    String getInputCsvUri();

    void setInputCsvUri(String value);

    @Default.Boolean(true)
    Boolean getWithExportVcfToBq();

    void setWithExportVcfToBq(Boolean value);

    @Default.Boolean(true)
    Boolean getWithVariantCalling();

    void setWithVariantCalling(Boolean value);

    @Description("Size of batch (MB) in which will be splited large FASTQ")
    @Default.Integer(0)
    Integer getFastqBatchSizeMB();

    void setFastqBatchSizeMB(Integer value);

    @Description("Size of batch (read count) in which will be splited large FASTQ")
    @Default.Integer(1000000)
    Integer getFastqBatchSizeReadCount();

    void setFastqBatchSizeReadCount(Integer value);
}

