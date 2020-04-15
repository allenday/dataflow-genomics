package com.google.allenday.genomics.core.integration;

import com.google.allenday.genomics.core.batch.BatchProcessingModule;
import com.google.allenday.genomics.core.batch.PreparingTransform;
import com.google.allenday.genomics.core.io.BaseUriProvider;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
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
                                    GenomicsOptions genomicsOptions,
                                    Integer maxFastqSizeMB,
                                    Integer maxFastqChunkSize,
                                    Integer bamRegionSize,
                                    BaseUriProvider baseUriProvider,
                                    boolean withFinalBamFile) {
        super(srcBucket, inputCsvUri, sraSamplesToFilter, sraSamplesToSkip, project, region,
                genomicsOptions, maxFastqSizeMB, maxFastqChunkSize, bamRegionSize, withFinalBamFile);
        this.baseUriProvider = baseUriProvider;
    }

    @Provides
    @Singleton
    public PreparingTransform provideGroupByPairedReadsAndFilter(NameProvider nameProvider) {
        return new PreparingTransform() {
            @Override
            public PCollection<KV<SampleMetaData, List<FileWrapper>>> expand(PCollection<KV<SampleMetaData, List<FileWrapper>>> input) {
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
