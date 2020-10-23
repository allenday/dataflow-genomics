package com.google.allenday.genomics.core.example;

import com.google.allenday.genomics.core.pipeline.GenomicsProcessingParams;
import com.google.allenday.genomics.core.pipeline.batch.BatchProcessingModule;
import com.google.allenday.genomics.core.pipeline.batch.BatchProcessingPipelineOptions;
import com.google.allenday.genomics.core.preparing.anomaly.DetectMissedFilesAndUnsupportedInstrumentTransform;
import com.google.allenday.genomics.core.preparing.custom.FastqInputResourcePreparingTransform;
import com.google.allenday.genomics.core.preparing.custom.PassPreparingTransform;
import com.google.allenday.genomics.core.preparing.custom.SraInputResourcePreparingTransform;
import com.google.allenday.genomics.core.preparing.runfile.uriprovider.BaseUriProvider;
import com.google.allenday.genomics.core.preparing.runfile.uriprovider.DefaultBaseUriProvider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.util.List;


public class GiabExampleAppModule extends BatchProcessingModule {

    public GiabExampleAppModule(String srcBucket,
                                String inputCsvUri,
                                List<String> sraSamplesToFilter,
                                List<String> sraSamplesToSkip,
                                String project,
                                String region,
                                GenomicsProcessingParams genomicsProcessingParams,
                                Integer maxFastqSizeMB,
                                Integer maxFastqChunkSize,
                                Integer bamRegionSize,
                                boolean withFinalMerge) {
        super(srcBucket, inputCsvUri, sraSamplesToFilter, sraSamplesToSkip, project, region,
                genomicsProcessingParams, maxFastqSizeMB, maxFastqChunkSize, bamRegionSize,
                withFinalMerge);
    }

    public GiabExampleAppModule(BatchProcessingPipelineOptions batchProcessingPipelineOptions) {
        this(
                batchProcessingPipelineOptions.getSrcBucket(),
                batchProcessingPipelineOptions.getInputCsvUri(),
                batchProcessingPipelineOptions.getSraSamplesToFilter(),
                batchProcessingPipelineOptions.getSraSamplesToSkip(),
                batchProcessingPipelineOptions.getProject(),
                batchProcessingPipelineOptions.getRegion(),
                GenomicsProcessingParams.fromAlignerPipelineOptions(batchProcessingPipelineOptions),
                batchProcessingPipelineOptions.getMaxFastqSizeMB(),
                batchProcessingPipelineOptions.getMaxFastqChunkSize(),
                batchProcessingPipelineOptions.getBamRegionSize(),
                batchProcessingPipelineOptions.getWithFinalMerge()
        );
    }


    @Provides
    @Singleton
    public FastqInputResourcePreparingTransform provideFastqInputResourcePreparingTransform(
            DetectMissedFilesAndUnsupportedInstrumentTransform transform) {
        return transform;
    }

    @Provides
    @Singleton
    public SraInputResourcePreparingTransform provideSraInputResourcePreparingTransform() {
        return new PassPreparingTransform();
    }

    @Provides
    @Singleton
    public BaseUriProvider provideBaseUriProvider() {
        return DefaultBaseUriProvider.withDefaultProviderRule(srcBucket);
    }
}
