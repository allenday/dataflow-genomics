package com.google.allenday.genomics.core.batch;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.io.UriProvider;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraParser;
import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
import com.google.allenday.genomics.core.processing.AlignAndPostProcessTransform;
import com.google.allenday.genomics.core.processing.align.AddReferenceDataSourceFn;
import com.google.allenday.genomics.core.processing.align.AlignFn;
import com.google.allenday.genomics.core.processing.align.AlignService;
import com.google.allenday.genomics.core.processing.align.AlignTransform;
import com.google.allenday.genomics.core.processing.dv.DeepVariantFn;
import com.google.allenday.genomics.core.processing.dv.DeepVariantService;
import com.google.allenday.genomics.core.processing.lifesciences.LifeSciencesService;
import com.google.allenday.genomics.core.processing.sam.CreateBamIndexFn;
import com.google.allenday.genomics.core.processing.sam.MergeFn;
import com.google.allenday.genomics.core.processing.sam.SamBamManipulationService;
import com.google.allenday.genomics.core.processing.sam.SortFn;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqFn;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqService;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.util.List;


public abstract class BatchProcessingModule extends AbstractModule {

    protected String srcBucket;
    protected String inputCsvUri;
    protected List<String> sraSamplesToFilter;
    protected List<String> sraSamplesToSkip;
    protected String project;
    protected String region;
    protected GenomicsOptions genomicsOptions;

    public BatchProcessingModule(String srcBucket, String inputCsvUri, List<String> sraSamplesToFilter,
                                 List<String> sraSamplesToSkip, String project, String region, GenomicsOptions genomicsOptions) {
        this.srcBucket = srcBucket;
        this.inputCsvUri = inputCsvUri;
        this.sraSamplesToFilter = sraSamplesToFilter;
        this.sraSamplesToSkip = sraSamplesToSkip;
        this.project = project;
        this.region = region;
        this.genomicsOptions = genomicsOptions;
    }

    @Provides
    @Singleton
    public NameProvider provideNameProvider() {
        return NameProvider.initialize();
    }


    @Provides
    @Singleton
    public ReferencesProvider provideReferencesProvider(FileUtils fileUtils) {
        return new ReferencesProvider(fileUtils);
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
    public AddReferenceDataSourceFn provideAddReferenceDataSourceFn() {
        if (genomicsOptions.getRefDataJsonString() != null) {
            return new AddReferenceDataSourceFn.Explicitly(genomicsOptions.getRefDataJsonString());
        } else if (genomicsOptions.getGeneReferences() != null && genomicsOptions.getAllReferencesDirGcsUri() != null) {
            return new AddReferenceDataSourceFn.FromNameAndDirPath(genomicsOptions.getAllReferencesDirGcsUri(),
                    genomicsOptions.getGeneReferences());
        } else {
            throw new RuntimeException("You must provide refDataJsonString or allReferencesDirGcsUri+gneReferences");
        }
    }

    @Provides
    @Singleton
    public AlignTransform provideAlignTransform(AlignFn alignFn, AddReferenceDataSourceFn addReferenceDataSourceFn) {
        return new AlignTransform("Align reads transform", alignFn, addReferenceDataSourceFn);
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
    public ParseSourceCsvTransform provideParseSourceCsvTransform(FileUtils fileUtils,
                                                                  SampleMetaData.Parser geneSampleMetaDataParser,
                                                                  UriProvider uriProvider,
                                                                  PreparingTransform preparingTransform) {

        ParseSourceCsvTransform parseSourceCsvTransform = new ParseSourceCsvTransform("Parse CSV", inputCsvUri,
                geneSampleMetaDataParser, uriProvider, fileUtils);
        parseSourceCsvTransform.setSraSamplesToFilter(sraSamplesToFilter);
        parseSourceCsvTransform.setSraSamplesToSkip(sraSamplesToSkip);
        parseSourceCsvTransform.setPreparingTransforms(preparingTransform);
        return parseSourceCsvTransform;
    }

    @Provides
    @Singleton
    public LifeSciencesService provideLifeSciencesService() {
        return new LifeSciencesService();
    }

    @Provides
    @Singleton
    public DeepVariantService provideDeepVariantService(LifeSciencesService lifeSciencesService) {
        return new DeepVariantService(lifeSciencesService, genomicsOptions.getDeepVariantOptions());
    }

    @Provides
    @Singleton
    public DeepVariantFn provideDeepVariantFn(DeepVariantService deepVariantService, FileUtils fileUtils, ReferencesProvider referencesProvider, NameProvider nameProvider) {

        return new DeepVariantFn(deepVariantService, fileUtils, referencesProvider,
                genomicsOptions.getResultBucket(), String.format(genomicsOptions.getDeepVariantOutputDirPattern(),
                nameProvider.getCurrentTimeInDefaultFormat()));
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
