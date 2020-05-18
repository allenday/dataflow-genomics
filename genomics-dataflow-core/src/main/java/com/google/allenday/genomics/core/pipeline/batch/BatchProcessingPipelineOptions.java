package com.google.allenday.genomics.core.pipeline.batch;

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

    @Description("Max size of fastq file (MB) that will be formed before Align step")
    @Default.Integer(-1)
    Integer getMaxFastqSizeMB();

    void setMaxFastqSizeMB(Integer value);

    @Description("Size of chunk (in reads count) in which will be splited large FASTQ")
    @Default.Integer(2000000)
    Integer getMaxFastqChunkSize();

    void setMaxFastqChunkSize(Integer value);

    @Description("Num of bases that will contain region splitted BAM file")
    @Default.Integer(5000000)
    Integer getBamRegionSize();

    void setBamRegionSize(Integer value);

    @Description("Specifies if there need to merge chunks BAM files into final sample BAM")
    @Default.Boolean(true)
    Boolean getWithFinalMerge();

    void setWithFinalMerge(Boolean value);


    /**
     * Not used from 0.0.12 ver
     **/
    @Deprecated()
    @Description("Extension of source fastq run files")
    @Default.String("FASTQ")
    String getFastqExt();

    void setFastqExt(String value);
}

