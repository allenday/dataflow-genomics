package com.google.allenday.nanostream.rice.vcf_to_bq;

import com.google.allenday.genomics.core.pipeline.GenomicsPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface NanostreamRicePipelineVcfToBqOptions extends GenomicsPipelineOptions {

    @Description("Path pattern of VCF files")
    @Validation.Required
    String getVcfPathPattern();

    void setVcfPathPattern(String value);

    @Description("GCS bucket name that contains VCF files")
    @Validation.Required
    String getSrcBucket();

    void setSrcBucket(String value);


    @Description("GCS bucket for storing working files")
    @Validation.Required
    String getWorkingBucket();

    void setWorkingBucket(String value);

    @Description("GCS dir for storing working files")
    @Validation.Required
    String getWorkingDir();

    void setWorkingDir(String value);
}
