package com.google.allenday.nanostream.cannabis;

import com.google.allenday.genomics.core.pipeline.GenomicsPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

import java.util.List;

public interface NanostreamCannabisPipelineOptions extends GenomicsPipelineOptions {

    @Description("Name of GCS bucket with all source data")
    @Validation.Required
    String getSrcBucket();

    void setSrcBucket(String value);

    @Description("GCS uri pattern of CSV files with input data")
    @Validation.Required
    String getInputCsvUri();

    void setInputCsvUri(String value);

    @Default.Boolean(true)
    Boolean getExportVcfToBq();

    void setExportVcfToBq(Boolean value);

    @Default.Boolean(true)
    Boolean getWithVariantCalling();

    void setWithVariantCalling(Boolean value);
}

