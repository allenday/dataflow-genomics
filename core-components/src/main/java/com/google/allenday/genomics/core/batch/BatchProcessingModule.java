package com.google.allenday.genomics.core.batch;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.io.FastqReader;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.io.UriProvider;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraParser;
import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
import com.google.allenday.genomics.core.processing.AlignAndPostProcessTransform;
import com.google.allenday.genomics.core.processing.SplitFastqIntoBatches;
import com.google.allenday.genomics.core.processing.align.*;
import com.google.allenday.genomics.core.processing.dv.DeepVariantFn;
import com.google.allenday.genomics.core.processing.dv.DeepVariantService;
import com.google.allenday.genomics.core.processing.lifesciences.LifeSciencesService;
import com.google.allenday.genomics.core.processing.sam.*;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqFn;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqService;
import com.google.allenday.genomics.core.reference.ReferenceProvider;
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
    protected Integer maxFastqSizeMB;
    protected Integer maxFastqChunkSize;

    public BatchProcessingModule(String srcBucket, String inputCsvUri, List<String> sraSamplesToFilter,
                                 List<String> sraSamplesToSkip, String project, String region,
                                 GenomicsOptions genomicsOptions, Integer maxFastqSizeMB,
                                 Integer maxFastqChunkSize) {
        this.srcBucket = srcBucket;
        this.inputCsvUri = inputCsvUri;
        this.sraSamplesToFilter = sraSamplesToFilter;
        this.sraSamplesToSkip = sraSamplesToSkip;
        this.project = project;
        this.region = region;
        this.genomicsOptions = genomicsOptions;
        this.maxFastqSizeMB = maxFastqSizeMB;
        this.maxFastqChunkSize = maxFastqChunkSize;
    }

    @Provides
    @Singleton
    public NameProvider provideNameProvider() {
        return NameProvider.initialize();
    }


    @Provides
    @Singleton
    public ReferenceProvider provideReferenceProvider(FileUtils fileUtils) {
        return new ReferenceProvider(fileUtils);
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
    public MergeAndIndexFn provideMergeAndIndexFn(SamBamManipulationService samBamManipulationService,
                                                  FileUtils fileUtils,
                                                  NameProvider nameProvider) {
        TransformIoHandler mergeIoHandler = new TransformIoHandler(genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getMergedOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
                genomicsOptions.getMemoryOutputLimit(), fileUtils);
        TransformIoHandler indexIoHandler = new TransformIoHandler(genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getBamIndexOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
                genomicsOptions.getMemoryOutputLimit(), fileUtils);

        return new MergeAndIndexFn(mergeIoHandler, indexIoHandler, samBamManipulationService, fileUtils);
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
    public AlignFn provideAlignFn(AlignService alignService, ReferenceProvider referencesProvider, FileUtils fileUtils, NameProvider nameProvider) {
        TransformIoHandler alignIoHandler = new TransformIoHandler(genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getAlignedOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
                genomicsOptions.getMemoryOutputLimit(), fileUtils);

        return new AlignFn(alignService, referencesProvider, alignIoHandler, fileUtils);
    }

    @Provides
    @Singleton
    public AlignAndSortFn provideAlignAndSortFn(SamBamManipulationService samBamManipulationService, AlignService alignService, ReferenceProvider referencesProvider,
                                                FileUtils fileUtils, NameProvider nameProvider) {
        TransformIoHandler alignIoHandler = new TransformIoHandler(genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getAlignedOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
                genomicsOptions.getMemoryOutputLimit(), fileUtils);
        TransformIoHandler sortIoHandler = new TransformIoHandler(genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getSortedOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
                genomicsOptions.getMemoryOutputLimit(), fileUtils);
        return new AlignAndSortFn(alignService, samBamManipulationService, referencesProvider, alignIoHandler, sortIoHandler, fileUtils);
    }


    @Provides
    @Singleton
    public AddReferenceDataSourceFn provideAddReferenceDataSourceFn() {
        if (genomicsOptions.getGeneReferences() != null && genomicsOptions.getAllReferencesDirGcsUri() != null) {
            return new AddReferenceDataSourceFn.FromNameAndDirPath(genomicsOptions.getAllReferencesDirGcsUri(),
                    genomicsOptions.getGeneReferences());
        } else {
            return new AddReferenceDataSourceFn.Explicitly(genomicsOptions.getRefDataJsonString());
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
    public DeepVariantFn provideDeepVariantFn(DeepVariantService deepVariantService, FileUtils fileUtils, ReferenceProvider referencesProvider, NameProvider nameProvider) {

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

    @Provides
    @Singleton
    public SplitFastqIntoBatches provideSplitFastqIntoBatches(SplitFastqIntoBatches.ReadFastqPartFn readFastqPartFn,
                                                              SplitFastqIntoBatches.BuildFastqContentFn buildFastqContentFn) {
        return new SplitFastqIntoBatches(readFastqPartFn, buildFastqContentFn);
    }


    @Provides
    @Singleton
    public SplitFastqIntoBatches.ReadFastqPartFn provideSplitFastqIntoBatches(FileUtils fileUtils, FastqReader fastqReader) {
        return new SplitFastqIntoBatches.ReadFastqPartFn(fileUtils, fastqReader, maxFastqChunkSize);
    }

    @Provides
    @Singleton
    public SplitFastqIntoBatches.BuildFastqContentFn provideBuildFastqContentFn() {
        return new SplitFastqIntoBatches.BuildFastqContentFn(maxFastqSizeMB);
    }
}
