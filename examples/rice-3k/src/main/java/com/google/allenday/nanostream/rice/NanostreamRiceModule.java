package com.google.allenday.nanostream.rice;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraParser;
import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
import com.google.allenday.genomics.core.processing.AlignAndPostProcessTransform;
import com.google.allenday.genomics.core.processing.DeepVariantService;
import com.google.allenday.genomics.core.processing.SamBamManipulationService;
import com.google.allenday.genomics.core.processing.align.AlignFn;
import com.google.allenday.genomics.core.processing.align.AlignService;
import com.google.allenday.genomics.core.processing.align.AlignTransform;
import com.google.allenday.genomics.core.processing.lifesciences.LifeSciencesService;
import com.google.allenday.genomics.core.processing.other.*;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqFn;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqService;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.allenday.nanostream.rice.anomaly.DetectAnomalyTransform;
import com.google.allenday.nanostream.rice.anomaly.RecognizePairedReadsWithAnomalyFn;
import com.google.allenday.nanostream.rice.io.RiceUriProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.util.List;

//TODO

/**
 *
 */
public class NanostreamRiceModule extends AbstractModule {

    private String srcBucket;
    private String inputCsvUri;
    private List<String> sraSamplesToFilter;
    private String project;
    private String region;

    private GenomicsOptions genomicsOptions;

    public NanostreamRiceModule(Builder builder) {
        this.inputCsvUri = builder.inputCsvUri;
        this.sraSamplesToFilter = builder.sraSamplesToFilter;
        this.genomicsOptions = builder.genomicsOptions;
        this.srcBucket = builder.srcBucket;
        this.project = builder.project;
        this.region = builder.region;
    }

    public static class Builder {
        private String srcBucket;
        private String inputCsvUri;
        private GenomicsOptions genomicsOptions;
        private String project;
        private String region;

        private List<String> sraSamplesToFilter;

        public Builder setInputCsvUri(String inputCsvUri) {
            this.inputCsvUri = inputCsvUri;
            return this;
        }

        public Builder setSraSamplesToFilter(List<String> sraSamplesToFilter) {
            this.sraSamplesToFilter = sraSamplesToFilter;
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

        public Builder setFromOptions(NanostreamRicePipelineOptions nanostreamPipelineOptions) {
            setInputCsvUri(nanostreamPipelineOptions.getInputCsvUri());
            setSraSamplesToFilter(nanostreamPipelineOptions.getSraSamplesToFilter());
            setGenomicsOptions(GenomicsOptions.fromAlignerPipelineOptions(nanostreamPipelineOptions));
            setSrcBucket(nanostreamPipelineOptions.getSrcBucket());
            region = nanostreamPipelineOptions.getRegion();
            project = nanostreamPipelineOptions.getProject();
            return this;
        }

        public NanostreamRiceModule build() {
            return new NanostreamRiceModule(this);
        }

    }

    @Provides
    @Singleton
    public NameProvider provideNameProvider() {
        return NameProvider.initialize();
    }

    @Provides
    @Singleton
    public RecognizePairedReadsWithAnomalyFn provideParseRiceDataFn(FileUtils fileUtils) {
        return new RecognizePairedReadsWithAnomalyFn(srcBucket, fileUtils);
    }

    @Provides
    @Singleton
    public DetectAnomalyTransform provideGroupByPairedReadsAndFilter(RecognizePairedReadsWithAnomalyFn recognizePairedReadsWithAnomalyFn,
                                                                     NameProvider nameProvider) {
        return new DetectAnomalyTransform("Filter anomaly and prepare for processing", genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getAnomalyOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()), recognizePairedReadsWithAnomalyFn);
    }

    @Provides
    @Singleton
    public ReferencesProvider provideReferencesProvider(FileUtils fileUtils) {
        return new ReferencesProvider(fileUtils, genomicsOptions.getAllReferencesDirGcsUri());
    }

    @Provides
    @Singleton
    public FileUtils provideFileUtils() {
        return new FileUtils();
    }

    @Provides
    @Singleton
    public CmdExecutor provideCmdExecutor() {
        return new CmdExecutor();
    }

    @Provides
    @Singleton
    public WorkerSetupService provideWorkerSetupService(CmdExecutor cmdExecutor) {
        return new WorkerSetupService(cmdExecutor);
    }

    @Provides
    @Singleton
    public AlignService provideAlignService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor, FileUtils fileUtils) {
        return new AlignService(workerSetupService, cmdExecutor, fileUtils);
    }

    @Provides
    @Singleton
    public SamBamManipulationService provideSamBamManipulationService(FileUtils fileUtils) {
        return new SamBamManipulationService(fileUtils);
    }

    @Provides
    @Singleton
    public MergeFn provideMergeFn(SamBamManipulationService samBamManipulationService, FileUtils fileUtils, NameProvider nameProvider) {
        TransformIoHandler mergeIoHandler = new TransformIoHandler(genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getMergedOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
                genomicsOptions.getMemoryOutputLimit(), fileUtils);

        return new MergeFn(mergeIoHandler, samBamManipulationService, fileUtils);
    }

    @Provides
    @Singleton
    public SortFn provideSortFn(SamBamManipulationService samBamManipulationService, FileUtils fileUtils, NameProvider nameProvider) {
        TransformIoHandler sortIoHandler = new TransformIoHandler(genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getSortedOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
                genomicsOptions.getMemoryOutputLimit(), fileUtils);

        return new SortFn(sortIoHandler, samBamManipulationService, fileUtils);
    }

    @Provides
    @Singleton
    public AlignFn provideAlignFn(AlignService alignService, ReferencesProvider referencesProvider, FileUtils fileUtils, NameProvider nameProvider) {
        TransformIoHandler alignIoHandler = new TransformIoHandler(genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getAlignedOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
                genomicsOptions.getMemoryOutputLimit(), fileUtils);

        return new AlignFn(alignService, referencesProvider, alignIoHandler, fileUtils);
    }

    @Provides
    @Singleton
    public AlignTransform provideAlignTransform(AlignFn alignFn) {
        return new AlignTransform("Align reads transform", alignFn, genomicsOptions.getGeneReferences());
    }

    @Provides
    @Singleton
    public CreateBamIndexFn provideCreateBamIndexFn(SamBamManipulationService samBamManipulationService, FileUtils fileUtils, NameProvider nameProvider) {
        TransformIoHandler indexIoHandler = new TransformIoHandler(genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getBamIndexOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
                genomicsOptions.getMemoryOutputLimit(), fileUtils);

        return new CreateBamIndexFn(indexIoHandler, samBamManipulationService, fileUtils);
    }

    @Provides
    @Singleton
    public AlignAndPostProcessTransform provideAlignAndPostProcessTransform(AlignTransform alignTransform, SortFn sortFn,
                                                                            MergeFn mergeFn, CreateBamIndexFn createBamIndexFn) {
        return new AlignAndPostProcessTransform("Align -> Sort -> Merge transform -> Create index",
                alignTransform,
                sortFn,
                mergeFn,
                createBamIndexFn);
    }

    @Provides
    @Singleton
    public SampleMetaData.Parser provideSampleMetaDataParser() {
        return new SraParser();
    }

    @Provides
    @Singleton
    public RiceUriProvider provideRiceUriProvider() {
        return RiceUriProvider.withDefaultProviderRule(srcBucket);
    }

    @Provides
    @Singleton
    public ParseSourceCsvTransform provideParseSourceCsvTransform(FileUtils fileUtils,
                                                                  SampleMetaData.Parser geneSampleMetaDataParser,
                                                                  RiceUriProvider riceUriProvider,
                                                                  DetectAnomalyTransform detectAnomalyTransform) {

        ParseSourceCsvTransform parseSourceCsvTransform = new ParseSourceCsvTransform("Parse CSV", inputCsvUri,
                geneSampleMetaDataParser, riceUriProvider, fileUtils);
        parseSourceCsvTransform.setSraSamplesToFilter(sraSamplesToFilter);
        parseSourceCsvTransform.setPreparingTransforms(detectAnomalyTransform);
        return parseSourceCsvTransform;
    }

    @Provides
    @Singleton
    public LifeSciencesService provideLifeSciencesService() {
        return new LifeSciencesService();
    }

    @Provides
    @Singleton
    public DeepVariantService provideDeepVariantService(ReferencesProvider referencesProvider, LifeSciencesService lifeSciencesService) {
        DeepVariantService deepVariantService = new DeepVariantService(referencesProvider, lifeSciencesService,
                genomicsOptions.getDeepVariantOptions());
        return deepVariantService;
    }

    @Provides
    @Singleton
    public DeepVariantFn provideDeepVariantFn(DeepVariantService deepVariantService, FileUtils fileUtils, NameProvider nameProvider) {

        return new DeepVariantFn(deepVariantService, fileUtils, genomicsOptions.getResultBucket(), String.format(genomicsOptions.getDeepVariantOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()));
    }

    @Provides
    @Singleton
    public VcfToBqService provideVcfToBqService(LifeSciencesService lifeSciencesService, NameProvider nameProvider) {
        VcfToBqService vcfToBqService = new VcfToBqService(lifeSciencesService, String.format("%s:%s", project, genomicsOptions.getVcfBqDatasetAndTablePattern()),
                genomicsOptions.getResultBucket(), genomicsOptions.getVcfToBqOutputDir(), nameProvider.getCurrentTimeInDefaultFormat());
        vcfToBqService.setRegion(region);
        return vcfToBqService;
    }


    @Provides
    @Singleton
    public VcfToBqFn provideVcfToBqFn(VcfToBqService vcfToBqService, FileUtils fileUtils) {

        return new VcfToBqFn(vcfToBqService, fileUtils);
    }

}
