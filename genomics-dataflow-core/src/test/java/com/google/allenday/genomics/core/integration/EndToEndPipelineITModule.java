package com.google.allenday.genomics.core.integration;

import com.google.allenday.genomics.core.pipeline.batch.BatchProcessingModule;
import com.google.allenday.genomics.core.pipeline.batch.PreparingTransform;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.runfile.FastqInputResource;
import com.google.allenday.genomics.core.preparing.runfile.uriprovider.BaseUriProvider;
import com.google.allenday.genomics.core.pipeline.GenomicsProcessingParams;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;


public class EndToEndPipelineITModule extends BatchProcessingModule {

    private BaseUriProvider baseUriProvider;

    public EndToEndPipelineITModule(String srcBucket,
                                    String inputCsvUri,
                                    List<String> sraSamplesToFilter,
                                    List<String> sraSamplesToSkip,
                                    String project,
                                    String region,
                                    GenomicsProcessingParams genomicsProcessingParams,
                                    Integer maxFastqSizeMB,
                                    Integer maxFastqChunkSize,
                                    Integer bamRegionSize,
                                    BaseUriProvider baseUriProvider,
                                    boolean withFinalBamFile) {
        super(srcBucket, inputCsvUri, sraSamplesToFilter, sraSamplesToSkip, project, region,
                genomicsProcessingParams, maxFastqSizeMB, maxFastqChunkSize, bamRegionSize, withFinalBamFile);
        this.baseUriProvider = baseUriProvider;
    }

    @Provides
    @Singleton
    public PreparingTransform provideGroupByPairedReadsAndFilter(NameProvider nameProvider) {
        return new PreparingTransform() {
            @Override
            public PCollection<KV<SampleRunMetaData, List<FastqInputResource>>> expand(PCollection<KV<SampleRunMetaData, List<FastqInputResource>>> input) {
                return input;
            }
        };
    }

    @Provides
    @Singleton
    public BaseUriProvider provideBaseUriProvider() {
        return baseUriProvider;
    }
}
