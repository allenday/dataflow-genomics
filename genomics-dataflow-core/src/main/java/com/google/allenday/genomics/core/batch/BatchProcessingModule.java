package com.google.allenday.genomics.core.batch;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.io.*;
import com.google.allenday.genomics.core.lifesciences.LifeSciencesService;
import com.google.allenday.genomics.core.model.Aligner;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraParser;
import com.google.allenday.genomics.core.model.VariantCaller;
import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
import com.google.allenday.genomics.core.processing.AlignAndSamProcessingTransform;
import com.google.allenday.genomics.core.processing.SamToolsService;
import com.google.allenday.genomics.core.processing.SplitFastqIntoBatches;
import com.google.allenday.genomics.core.processing.align.*;
import com.google.allenday.genomics.core.processing.index.CreateBamIndexFn;
import com.google.allenday.genomics.core.processing.merge.MergeFn;
import com.google.allenday.genomics.core.processing.split.BatchSamParser;
import com.google.allenday.genomics.core.processing.sort.SortFn;
import com.google.allenday.genomics.core.processing.split.SamIntoRegionBatchesFn;
import com.google.allenday.genomics.core.processing.variantcall.*;
import com.google.allenday.genomics.core.processing.vcf_to_bq.PrepareAndExecuteVcfToBqTransform;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqFn;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqService;
import com.google.allenday.genomics.core.reference.ReferenceProvider;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.lang.annotation.Retention;
import java.util.List;

import static java.lang.annotation.RetentionPolicy.RUNTIME;


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
    protected UriProvider.FastqExt fastqExt;
    protected Integer bamRegionSize;
    protected boolean withFinalMerge;

    public BatchProcessingModule(String srcBucket, String inputCsvUri, List<String> sraSamplesToFilter,
                                 List<String> sraSamplesToSkip, String project, String region,
                                 GenomicsOptions genomicsOptions, Integer maxFastqSizeMB,
                                 Integer maxFastqChunkSize, UriProvider.FastqExt fastqExt,
                                 Integer bamRegionSize, boolean withFinalMerge) {
        this.srcBucket = srcBucket;
        this.inputCsvUri = inputCsvUri;
        this.sraSamplesToFilter = sraSamplesToFilter;
        this.sraSamplesToSkip = sraSamplesToSkip;
        this.project = project;
        this.region = region;
        this.genomicsOptions = genomicsOptions;
        this.maxFastqSizeMB = maxFastqSizeMB;
        this.maxFastqChunkSize = maxFastqChunkSize;
        this.bamRegionSize = bamRegionSize;
        this.fastqExt = fastqExt;
        this.withFinalMerge = withFinalMerge;
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
    public Minimap2AlignService provideMinimap2AlignService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor, FileUtils fileUtils) {
        return new Minimap2AlignService(workerSetupService, cmdExecutor, fileUtils);
    }

    @Provides
    @Singleton
    public BwaAlignService provideBwaAlignService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor, FileUtils fileUtils) {
        return new BwaAlignService(workerSetupService, cmdExecutor, fileUtils);
    }

    @Provides
    @Singleton
    public AlignService provideAlignService(Minimap2AlignService minimap2AlignService, BwaAlignService bwaAlignService) {
        if (genomicsOptions.getAligner().equals(Aligner.MINIMAP2)) {
            return minimap2AlignService;
        } else if (genomicsOptions.getAligner().equals(Aligner.BWA)) {
            return bwaAlignService;
        } else {
            throw new IllegalArgumentException(String.format("Aligner %s is not supported", genomicsOptions.getAligner()));
        }
    }

    @Provides
    @Singleton
    public SamToolsService provideSamBamManipulationService(FileUtils fileUtils) {
        return new SamToolsService(fileUtils);
    }

    @Provides
    public TransformIoHandler provideTransformIoHandler(FileUtils fileUtils, NameProvider nameProvider) {
        return new TransformIoHandler(genomicsOptions.getResultBucket(), fileUtils, nameProvider.getCurrentTimeInDefaultFormat());
    }

    @Provides
    @Singleton
    @MergeRegions
    public MergeFn provideRegionsMergeFn(SamToolsService samToolsService, FileUtils fileUtils,
                                         TransformIoHandler transformIoHandler) {
        transformIoHandler.overwriteWithTimestampedDestGcsDir(genomicsOptions.getMergedRegionsDirPattern());
        return new MergeFn(transformIoHandler, samToolsService, fileUtils);
    }

    @Provides
    @Singleton
    @MergeFinal
    public MergeFn provideFinalMergeFn(SamToolsService samToolsService, FileUtils fileUtils,
                                       TransformIoHandler transformIoHandler) {
        transformIoHandler.overwriteWithTimestampedDestGcsDir(genomicsOptions.getFinalMergedDirPattern());
        return new MergeFn(transformIoHandler, samToolsService, fileUtils);
    }

    @Provides
    @Singleton
    public SortFn provideSortFn(SamToolsService samToolsService, FileUtils fileUtils,
                                TransformIoHandler sortIoHandler) {
        sortIoHandler.overwriteWithTimestampedDestGcsDir(genomicsOptions.getSortedOutputDirPattern());
        return new SortFn(sortIoHandler, samToolsService, fileUtils);
    }

    @Provides
    @Singleton
    public AlignFn provideAlignFn(AlignService alignService, ReferenceProvider referencesProvider, FileUtils fileUtils,
                                  TransformIoHandler alignIoHandler) {
        alignIoHandler.overwriteWithTimestampedDestGcsDir(genomicsOptions.getAlignedOutputDirPattern());
        alignIoHandler.setMemoryOutputLimitMb(genomicsOptions.getMemoryOutputLimit());
        return new AlignFn(alignService, referencesProvider, alignIoHandler, fileUtils);
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
    public CreateBamIndexFn provideCreateBamIndexFn(SamToolsService samToolsService, FileUtils fileUtils,
                                                    TransformIoHandler indexIoHandler) {
        indexIoHandler.overwriteWithTimestampedDestGcsDir(genomicsOptions.getMergedRegionsDirPattern());
        return new CreateBamIndexFn(indexIoHandler, samToolsService, fileUtils);
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
    public GATKService provideGATKService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor) {
        return new GATKService(workerSetupService, cmdExecutor);
    }

    @Provides
    @Singleton
    public VariantCallingService provideVariantCallingService(DeepVariantService deepVariantService, GATKService gatkService) {
        if (genomicsOptions.getVariantCaller().equals(VariantCaller.GATK)) {
            return gatkService;
        } else if (genomicsOptions.getVariantCaller().equals(VariantCaller.DEEP_VARIANT)) {
            return deepVariantService;
        } else {
            throw new IllegalArgumentException(String.format("Variant Caller %s is not supported", genomicsOptions.getVariantCaller()));
        }
    }

    @Provides
    @Singleton
    public VariantCallingFn provideDeepVariantFn(VariantCallingService variantCallingService, FileUtils fileUtils, ReferenceProvider referencesProvider, NameProvider nameProvider) {

        return new VariantCallingFn(
                variantCallingService,
                fileUtils,
                referencesProvider,
                genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getVariantCallingOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()));
    }

    @Provides
    @Singleton
    public VcfToBqService provideVcfToBqService(LifeSciencesService lifeSciencesService, NameProvider nameProvider) {
        VcfToBqService vcfToBqService = new VcfToBqService(
                lifeSciencesService,
                String.format("%s:%s", project, genomicsOptions.getVcfBqDatasetAndTablePattern()),
                genomicsOptions.getResultBucket(),
                String.format(genomicsOptions.getVcfToBqOutputDir(), nameProvider.getCurrentTimeInDefaultFormat()),
                nameProvider.getCurrentTimeInDefaultFormat());
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
        return new SplitFastqIntoBatches(readFastqPartFn, buildFastqContentFn, maxFastqSizeMB);
    }


    @Provides
    @Singleton
    public SplitFastqIntoBatches.ReadFastqPartFn provideSplitFastqIntoBatches(FileUtils fileUtils,
                                                                              FastqReader fastqReader,
                                                                              TransformIoHandler splitFastqIntoBatchesIoHandler) {
        splitFastqIntoBatchesIoHandler.overwriteWithTimestampedDestGcsDir(genomicsOptions.getChuncksByCountOutputDirPattern());
        splitFastqIntoBatchesIoHandler.setMemoryOutputLimitMb(genomicsOptions.getMemoryOutputLimit());
        return new SplitFastqIntoBatches.ReadFastqPartFn(fileUtils, fastqReader, splitFastqIntoBatchesIoHandler, maxFastqChunkSize, maxFastqSizeMB);
    }


    @Provides
    @Singleton
    public BatchSamParser provideBatchSamParser(SamToolsService samToolsService, FileUtils fileUtils) {
        return new BatchSamParser(samToolsService, fileUtils);
    }

    @Provides
    @Singleton
    public SamIntoRegionBatchesFn provideParseSamRecordsFn(FileUtils fileUtils,
                                                           IoUtils ioUtils,
                                                           BatchSamParser batchSamParser,
                                                           SamToolsService samToolsService,
                                                           TransformIoHandler sortAndSplitIoHandler) {
        sortAndSplitIoHandler.overwriteWithTimestampedDestGcsDir(genomicsOptions.getSortedAndSplittedOutputDirPattern());
        return new SamIntoRegionBatchesFn(sortAndSplitIoHandler, samToolsService, batchSamParser, fileUtils,
                ioUtils, bamRegionSize);
    }

    @Provides
    @Singleton
    public SplitFastqIntoBatches.BuildFastqContentFn provideBuildFastqContentFn(FileUtils fileUtils, IoUtils ioUtils,
                                                                                TransformIoHandler buildFastqContentIoHandler) {
        buildFastqContentIoHandler.overwriteWithTimestampedDestGcsDir(genomicsOptions.getChuncksBySizeOutputDirPattern());
        buildFastqContentIoHandler.setMemoryOutputLimitMb(genomicsOptions.getMemoryOutputLimit());
        return new SplitFastqIntoBatches.BuildFastqContentFn(buildFastqContentIoHandler, fileUtils, ioUtils, maxFastqSizeMB);
    }

    @Provides
    @Singleton
    public AlignAndSamProcessingTransform.FinalMergeTransform provideFinalMergeTransform(@MergeFinal MergeFn mergeFn) {
        return new AlignAndSamProcessingTransform.FinalMergeTransform(mergeFn);
    }

    @Provides
    @Singleton
    public AlignAndSamProcessingTransform provideAlignAndPostProcessTransform(AlignTransform alignTransform,
                                                                              SamIntoRegionBatchesFn samIntoRegionBatchesFn,
                                                                              @MergeRegions MergeFn mergeFn,
                                                                              AlignAndSamProcessingTransform.FinalMergeTransform finalMergeTransform,
                                                                              CreateBamIndexFn createBamIndexFn) {
        return new AlignAndSamProcessingTransform(alignTransform, samIntoRegionBatchesFn, mergeFn, finalMergeTransform, createBamIndexFn, withFinalMerge);
    }

    @Provides
    @Singleton
    public VariantCallingTransform provideVariantCallingTransform(VariantCallingFn variantCallingFn) {
        return new VariantCallingTransform(variantCallingFn);
    }

    @Provides
    @Singleton
    public PrepareAndExecuteVcfToBqTransform providePrepareAndExecuteVcfToBqTransform(VcfToBqFn vcfToBqFn) {
        return new PrepareAndExecuteVcfToBqTransform(vcfToBqFn);
    }

    @Retention(RUNTIME)
    @BindingAnnotation
    public @interface MergeRegions {
    }


    @Retention(RUNTIME)
    @BindingAnnotation
    public @interface MergeFinal {
    }
}
