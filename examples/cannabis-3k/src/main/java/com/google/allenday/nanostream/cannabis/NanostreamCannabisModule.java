package com.google.allenday.nanostream.cannabis;

import com.google.allenday.genomics.core.batch.BatchProcessingModule;
import com.google.allenday.genomics.core.batch.BatchProcessingPipelineOptions;
import com.google.allenday.genomics.core.batch.PreparingTransform;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.UriProvider;
import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
import com.google.allenday.genomics.core.processing.align.AlignService;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.allenday.nanostream.cannabis.anomaly.DetectAnomalyTransform;
import com.google.allenday.nanostream.cannabis.anomaly.RecognizePairedReadsWithAnomalyFn;
import com.google.allenday.nanostream.cannabis.io.CannabisUriProvider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.util.List;


public class NanostreamCannabisModule extends BatchProcessingModule {

    public NanostreamCannabisModule(String srcBucket,
                                    String inputCsvUri,
                                    List<String> sraSamplesToFilter,
                                    List<String> sraSamplesToSkip,
                                    String project,
                                    String region,
                                    GenomicsOptions genomicsOptions) {
        super(srcBucket, inputCsvUri, sraSamplesToFilter, sraSamplesToSkip, project, region, genomicsOptions);
    }

    public static class Builder {
        private String srcBucket;
        private String inputCsvUri;
        private GenomicsOptions genomicsOptions;
        private String project;
        private String region;

        private List<String> sraSamplesToFilter;
        private List<String> sraSamplesToSkip;

        public Builder setInputCsvUri(String inputCsvUri) {
            this.inputCsvUri = inputCsvUri;
            return this;
        }

        public Builder setSraSamplesToFilter(List<String> sraSamplesToFilter) {
            this.sraSamplesToFilter = sraSamplesToFilter;
            return this;
        }

        public Builder setSraSamplesToSkip(List<String> sraSamplesToSkip) {
            this.sraSamplesToSkip = sraSamplesToSkip;
            return this;
        }

        public Builder setGenomicsOptions(GenomicsOptions genomicsOptions) {
            this.genomicsOptions = genomicsOptions;
            return this;
        }

        public Builder setSrcBucket(String srcBucket) {
            this.srcBucket = srcBucket;
            return this;
        }

        public Builder setFromOptions(BatchProcessingPipelineOptions batchProcessingPipelineOptions) {
            setInputCsvUri(batchProcessingPipelineOptions.getInputCsvUri());
            setSraSamplesToFilter(batchProcessingPipelineOptions.getSraSamplesToFilter());
            setSraSamplesToSkip(batchProcessingPipelineOptions.getSraSamplesToSkip());
            setGenomicsOptions(GenomicsOptions.fromAlignerPipelineOptions(batchProcessingPipelineOptions));
            setSrcBucket(batchProcessingPipelineOptions.getSrcBucket());
            region = batchProcessingPipelineOptions.getRegion();
            project = batchProcessingPipelineOptions.getProject();
            return this;
        }

        public NanostreamCannabisModule build() {
            return new NanostreamCannabisModule(srcBucket, inputCsvUri, sraSamplesToFilter, sraSamplesToSkip, project, region, genomicsOptions);
        }

    }

    @Provides
    @Singleton
    public RecognizePairedReadsWithAnomalyFn provideParseCannabisDataFn(FileUtils fileUtils) {
        return new RecognizePairedReadsWithAnomalyFn(srcBucket, fileUtils);
    }

    @Provides
    @Singleton
    public PreparingTransform provideGroupByPairedReadsAndFilter(RecognizePairedReadsWithAnomalyFn recognizePairedReadsWithAnomalyFn,
                                                                 NameProvider nameProvider) {
        return new DetectAnomalyTransform("Filter anomaly and prepare for processing", genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getAnomalyOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()), recognizePairedReadsWithAnomalyFn);
    }

    @Provides
    @Singleton
    public UriProvider provideCannabisUriProvider() {
        return CannabisUriProvider.withDefaultProviderRule(srcBucket);
    }

    @Provides
    @Singleton
    public AlignService.Instrument provideInstrument() {
        return AlignService.Instrument.ILLUMINA;
    }
}
