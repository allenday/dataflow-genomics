package com.google.allenday.genomics.core.pipeline.batch;

import com.google.allenday.genomics.core.export.vcftobq.PrepareAndExecuteVcfToBqTransform;
import com.google.allenday.genomics.core.export.vcftobq.VcfToBqFn;
import com.google.allenday.genomics.core.export.vcftobq.VcfToBqService;
import com.google.allenday.genomics.core.gcp.LifeSciencesService;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.pipeline.GenomicsProcessingParams;
import com.google.allenday.genomics.core.pipeline.io.TransformIoHandler;
import com.google.allenday.genomics.core.preparing.RetrieveFastqFromCsvTransform;
import com.google.allenday.genomics.core.preparing.anomaly.DetectMissedFilesAndUnsupportedInstrumentTransform;
import com.google.allenday.genomics.core.preparing.anomaly.RecognizeMissedRunFilesFn;
import com.google.allenday.genomics.core.preparing.anomaly.RecognizeUnsupportedInstrumentFn;
import com.google.allenday.genomics.core.preparing.custom.FastqInputResourcePreparingTransform;
import com.google.allenday.genomics.core.preparing.custom.SraInputResourcePreparingTransform;
import com.google.allenday.genomics.core.preparing.fastq.BuildFastqContentFn;
import com.google.allenday.genomics.core.preparing.fastq.FastqReader;
import com.google.allenday.genomics.core.preparing.fastq.ReadFastqAndSplitIntoChunksFn;
import com.google.allenday.genomics.core.preparing.metadata.EnrichWithFastqRunInputResourceFn;
import com.google.allenday.genomics.core.preparing.metadata.EnrichWithSraInputResourceFn;
import com.google.allenday.genomics.core.preparing.metadata.shema.CsvSchema;
import com.google.allenday.genomics.core.preparing.metadata.shema.FullCsvSchemaImpl;
import com.google.allenday.genomics.core.preparing.runfile.uriprovider.BaseUriProvider;
import com.google.allenday.genomics.core.preparing.sra.SraToolsService;
import com.google.allenday.genomics.core.processing.AlignAndSamProcessingTransform;
import com.google.allenday.genomics.core.processing.align.*;
import com.google.allenday.genomics.core.processing.sam.SamToolsService;
import com.google.allenday.genomics.core.processing.sam.index.CreateBamIndexFn;
import com.google.allenday.genomics.core.processing.sam.merge.MergeFn;
import com.google.allenday.genomics.core.processing.sam.sort.SortFn;
import com.google.allenday.genomics.core.processing.sam.split.BatchSamParser;
import com.google.allenday.genomics.core.processing.sam.split.SamIntoRegionBatchesFn;
import com.google.allenday.genomics.core.processing.variantcall.*;
import com.google.allenday.genomics.core.reference.ReferenceProvider;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.allenday.genomics.core.worker.WorkerSetupService;
import com.google.allenday.genomics.core.worker.cmd.CmdExecutor;
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
    protected GenomicsProcessingParams genomicsParams;
    protected Integer maxFastqSizeMB;
    protected Integer maxFastqChunkSize;
    protected Integer bamRegionSize;
    protected boolean withFinalMerge;

    public BatchProcessingModule(String srcBucket, String inputCsvUri, List<String> sraSamplesToFilter,
                                 List<String> sraSamplesToSkip, String project, String region,
                                 GenomicsProcessingParams genomicsParams, Integer maxFastqSizeMB,
                                 Integer maxFastqChunkSize,
                                 Integer bamRegionSize, boolean withFinalMerge) {
        this.srcBucket = srcBucket;
        this.inputCsvUri = inputCsvUri;
        this.sraSamplesToFilter = sraSamplesToFilter;
        this.sraSamplesToSkip = sraSamplesToSkip;
        this.project = project;
        this.region = region;
        this.genomicsParams = genomicsParams;
        this.maxFastqSizeMB = maxFastqSizeMB;
        this.maxFastqChunkSize = maxFastqChunkSize;
        this.bamRegionSize = bamRegionSize;
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
        if (genomicsParams.getAligner().equals(Aligner.MINIMAP2)) {
            return minimap2AlignService;
        } else if (genomicsParams.getAligner().equals(Aligner.BWA)) {
            return bwaAlignService;
        } else {
            throw new IllegalArgumentException(String.format("Aligner %s is not supported", genomicsParams.getAligner()));
        }
    }

    @Provides
    @Singleton
    public SamToolsService provideSamBamManipulationService(FileUtils fileUtils) {
        return new SamToolsService(fileUtils);
    }

    @Provides
    public TransformIoHandler provideTransformIoHandler(FileUtils fileUtils, NameProvider nameProvider) {
        return new TransformIoHandler(genomicsParams.getResultBucket(), fileUtils, nameProvider.getCurrentTimeInDefaultFormat());
    }

    @Provides
    @Singleton
    @MergeRegions
    public MergeFn provideRegionsMergeFn(SamToolsService samToolsService, FileUtils fileUtils,
                                         TransformIoHandler transformIoHandler) {
        transformIoHandler.overwriteWithTimestampedDestGcsDir(genomicsParams.getMergedRegionsDirPattern());
        return new MergeFn(transformIoHandler, samToolsService, fileUtils);
    }

    @Provides
    @Singleton
    @MergeFinal
    public MergeFn provideFinalMergeFn(SamToolsService samToolsService, FileUtils fileUtils,
                                       TransformIoHandler transformIoHandler) {
        transformIoHandler.overwriteWithTimestampedDestGcsDir(genomicsParams.getFinalMergedDirPattern());
        return new MergeFn(transformIoHandler, samToolsService, fileUtils);
    }

    @Provides
    @Singleton
    public SortFn provideSortFn(SamToolsService samToolsService, FileUtils fileUtils,
                                TransformIoHandler sortIoHandler) {
        sortIoHandler.overwriteWithTimestampedDestGcsDir(genomicsParams.getSortedOutputDirPattern());
        return new SortFn(sortIoHandler, samToolsService, fileUtils);
    }

    @Provides
    @Singleton
    public AlignFn provideAlignFn(AlignService alignService, ReferenceProvider referencesProvider, FileUtils fileUtils,
                                  TransformIoHandler alignIoHandler) {
        alignIoHandler.overwriteWithTimestampedDestGcsDir(genomicsParams.getAlignedOutputDirPattern());
        alignIoHandler.setMemoryOutputLimitMb(genomicsParams.getMemoryOutputLimit());
        return new AlignFn(alignService, referencesProvider, alignIoHandler, fileUtils);
    }


    @Provides
    @Singleton
    public AddReferenceDataSourceFn provideAddReferenceDataSourceFn() {
        if (genomicsParams.getGeneReferences() != null && genomicsParams.getAllReferencesDirGcsUri() != null) {
            return new AddReferenceDataSourceFn.FromNameAndDirPath(genomicsParams.getAllReferencesDirGcsUri(),
                    genomicsParams.getGeneReferences());
        } else {
            return new AddReferenceDataSourceFn.Explicitly(genomicsParams.getRefDataJsonString());
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
        indexIoHandler.overwriteWithTimestampedDestGcsDir(genomicsParams.getMergedRegionsDirPattern());
        return new CreateBamIndexFn(indexIoHandler, samToolsService, fileUtils);
    }

    @Provides
    @Singleton
    public CsvSchema provideCsvSchema() {
        return new FullCsvSchemaImpl();
    }

    @Provides
    @Singleton
    public SampleRunMetaData.Parser provideSampleMetaDataParser(CsvSchema csvSchema) {
        return new SampleRunMetaData.Parser(csvSchema);
    }

    @Provides
    @Singleton
    public LifeSciencesService provideLifeSciencesService() {
        return new LifeSciencesService();
    }

    @Provides
    @Singleton
    public DeepVariantService provideDeepVariantService(LifeSciencesService lifeSciencesService) {
        return new DeepVariantService(lifeSciencesService, genomicsParams.getDeepVariantOptions());
    }

    @Provides
    @Singleton
    public GATKService provideGATKService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor) {
        return new GATKService(workerSetupService, cmdExecutor);
    }

    @Provides
    @Singleton
    public VariantCallingService provideVariantCallingService(DeepVariantService deepVariantService, GATKService gatkService) {
        if (genomicsParams.getVariantCaller().equals(VariantCaller.GATK)) {
            return gatkService;
        } else if (genomicsParams.getVariantCaller().equals(VariantCaller.DEEP_VARIANT)) {
            return deepVariantService;
        } else {
            throw new IllegalArgumentException(String.format("Variant Caller %s is not supported", genomicsParams.getVariantCaller()));
        }
    }

    @Provides
    @Singleton
    public VariantCallingFn provideDeepVariantFn(VariantCallingService variantCallingService, FileUtils fileUtils, ReferenceProvider referencesProvider, NameProvider nameProvider) {

        return new VariantCallingFn(
                variantCallingService,
                fileUtils,
                referencesProvider,
                genomicsParams.getResultBucket(),
                String.format(genomicsParams.getVariantCallingOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()));
    }

    @Provides
    @Singleton
    public VcfToBqService provideVcfToBqService(LifeSciencesService lifeSciencesService, NameProvider nameProvider) {
        VcfToBqService vcfToBqService = new VcfToBqService(
                lifeSciencesService,
                String.format("%s:%s", project, genomicsParams.getVcfBqDatasetAndTablePattern()),
                genomicsParams.getResultBucket(),
                String.format(genomicsParams.getVcfToBqOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
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
    public EnrichWithFastqRunInputResourceFn provideEnrichWithFastqRunInputResourceFn(BaseUriProvider baseUriProvider,
                                                                                      FileUtils fileUtils) {
        return new EnrichWithFastqRunInputResourceFn(baseUriProvider, fileUtils);
    }

    @Provides
    @Singleton
    public EnrichWithSraInputResourceFn provideEnrichWithSraInputResourceFn() {
        return new EnrichWithSraInputResourceFn();
    }

    @Provides
    @Singleton
    public ReadFastqAndSplitIntoChunksFn.FromFastqInputResource provideFromFastqInputResource(FileUtils fileUtils,
                                                                                              FastqReader fastqReader,
                                                                                              TransformIoHandler splitFastqIntoBatchesIoHandler) {
        splitFastqIntoBatchesIoHandler.overwriteWithTimestampedDestGcsDir(genomicsParams.getChuncksByCountOutputDirPattern());
        splitFastqIntoBatchesIoHandler.setMemoryOutputLimitMb(genomicsParams.getMemoryOutputLimit());
        return new ReadFastqAndSplitIntoChunksFn.FromFastqInputResource(fileUtils, fastqReader, splitFastqIntoBatchesIoHandler,
                maxFastqChunkSize, maxFastqSizeMB > 0);
    }

    @Provides
    @Singleton
    public SraToolsService provideSraToolsService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor,
                                                  FileUtils fileUtils) {
        return new SraToolsService(workerSetupService, cmdExecutor, fileUtils);
    }

    @Provides
    @Singleton
    public ReadFastqAndSplitIntoChunksFn.FromSraInputResource provideFromSraInputResource(FileUtils fileUtils,
                                                                                          FastqReader fastqReader,
                                                                                          SraToolsService sraToolsService,
                                                                                          TransformIoHandler splitFastqIntoBatchesIoHandler) {
        splitFastqIntoBatchesIoHandler.overwriteWithTimestampedDestGcsDir(genomicsParams.getChuncksByCountOutputDirPattern());
        splitFastqIntoBatchesIoHandler.setMemoryOutputLimitMb(genomicsParams.getMemoryOutputLimit());
        return new ReadFastqAndSplitIntoChunksFn.FromSraInputResource(fileUtils, fastqReader, splitFastqIntoBatchesIoHandler,
                sraToolsService, maxFastqChunkSize, maxFastqSizeMB > 0);
    }

    @Provides
    @Singleton
    public RetrieveFastqFromCsvTransform provideRetrieveFastqFromCsvTransform(SampleRunMetaData.Parser parser,
                                                                              EnrichWithFastqRunInputResourceFn
                                                                                      enrichWithFastqRunInputResourceFn,
                                                                              EnrichWithSraInputResourceFn
                                                                                      enrichWithSraInputResourceFn,
                                                                              FastqInputResourcePreparingTransform
                                                                                          fastqInputResourcePreparingTransform,
                                                                              SraInputResourcePreparingTransform
                                                                                          sraInputResourcePreparingTransform,
                                                                              ReadFastqAndSplitIntoChunksFn.FromFastqInputResource
                                                                                      fromFastqInputResource,
                                                                              ReadFastqAndSplitIntoChunksFn.FromSraInputResource
                                                                                      fromSraInputResource,
                                                                              BuildFastqContentFn buildFastqContentFn) {
        return new RetrieveFastqFromCsvTransform(
                inputCsvUri,
                parser,
                enrichWithFastqRunInputResourceFn,
                enrichWithSraInputResourceFn,
                fromFastqInputResource,
                fromSraInputResource, buildFastqContentFn,
                maxFastqSizeMB > 0)
                .withSraSamplesToFilter(sraSamplesToFilter)
                .withSraSamplesToSkip(sraSamplesToSkip)
                .withFastqInputResourcePreparingTransforms(fastqInputResourcePreparingTransform)
                .withSraInputResourcePreparingTransforms(sraInputResourcePreparingTransform);
    }

    @Provides
    @Singleton
    public FastqReader provideFastqReader(SamToolsService samToolsService) {
        return new FastqReader(samToolsService);
    }

    @Provides
    @Singleton
    public BatchSamParser provideBatchSamParser(SamToolsService samToolsService, FileUtils fileUtils) {
        return new BatchSamParser(samToolsService, fileUtils);
    }

    @Provides
    @Singleton
    public SamIntoRegionBatchesFn provideParseSamRecordsFn(FileUtils fileUtils,
                                                           BatchSamParser batchSamParser,
                                                           SamToolsService samToolsService,
                                                           TransformIoHandler sortAndSplitIoHandler) {
        sortAndSplitIoHandler.overwriteWithTimestampedDestGcsDir(genomicsParams.getSortedAndSplittedOutputDirPattern());
        return new SamIntoRegionBatchesFn(sortAndSplitIoHandler, samToolsService, batchSamParser, fileUtils, bamRegionSize);
    }

    @Provides
    @Singleton
    public BuildFastqContentFn provideBuildFastqContentFn(FileUtils fileUtils, TransformIoHandler buildFastqContentIoHandler) {
        buildFastqContentIoHandler.overwriteWithTimestampedDestGcsDir(genomicsParams.getChuncksBySizeOutputDirPattern());
        buildFastqContentIoHandler.setMemoryOutputLimitMb(genomicsParams.getMemoryOutputLimit());
        return new BuildFastqContentFn(buildFastqContentIoHandler, fileUtils, maxFastqSizeMB);
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


    @Provides
    @Singleton
    public RecognizeMissedRunFilesFn provideRecognizeMissedRunFilesFn(FileUtils fileUtils) {
        return new RecognizeMissedRunFilesFn(fileUtils);
    }

    @Provides
    @Singleton
    public RecognizeUnsupportedInstrumentFn provideRecognizeUnsupportedInstrumentFn() {
        return new RecognizeUnsupportedInstrumentFn();
    }


    @Provides
    @Singleton
    public DetectMissedFilesAndUnsupportedInstrumentTransform provideDetectMissedFilesAndUnsupportedInstrumentTransform(
            NameProvider nameProvider,
            RecognizeUnsupportedInstrumentFn recognizeUnsupportedInstrumentFn,
            RecognizeMissedRunFilesFn recognizeMissedRunFilesFn
    ) {
        return new DetectMissedFilesAndUnsupportedInstrumentTransform(genomicsParams.getResultBucket(),
                String.format(genomicsParams.getAnomalyOutputDirPattern(), nameProvider.getCurrentTimeInDefaultFormat()),
                recognizeUnsupportedInstrumentFn,
                recognizeMissedRunFilesFn);
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
