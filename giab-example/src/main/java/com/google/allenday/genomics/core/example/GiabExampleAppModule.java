package com.google.allenday.genomics.core.example;

import com.google.allenday.genomics.core.batch.BatchProcessingModule;
import com.google.allenday.genomics.core.batch.BatchProcessingPipelineOptions;
import com.google.allenday.genomics.core.batch.PreparingTransform;
import com.google.allenday.genomics.core.io.BaseUriProvider;
import com.google.allenday.genomics.core.io.DefaultBaseUriProvider;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.pipeline.GenomicsProcessingParams;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

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
        return DefaultBaseUriProvider.withDefaultProviderRule(srcBucket);
    }
}
