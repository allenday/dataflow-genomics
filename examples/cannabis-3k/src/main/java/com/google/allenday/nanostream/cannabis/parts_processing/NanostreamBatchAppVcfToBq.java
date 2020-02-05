package com.google.allenday.nanostream.cannabis.parts_processing;

import com.google.allenday.genomics.core.batch.BatchProcessingPipelineOptions;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.IoUtils;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.pipeline.PipelineSetupUtils;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqFn;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.allenday.nanostream.cannabis.NanostreamCannabisModule;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class NanostreamBatchAppVcfToBq {
    private static Logger LOG = LoggerFactory.getLogger(NanostreamBatchAppVcfToBq.class);

    private final static String JOB_NAME_PREFIX = "nanostream-cannabis--vcf-to-bq";

    public static boolean hasFilter(List<String> sraSamplesToFilter) {
        return sraSamplesToFilter != null && sraSamplesToFilter.size() > 0;
    }

    public static void main(String[] args) {

        BatchProcessingPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(BatchProcessingPipelineOptions.class);
        PipelineSetupUtils.prepareForInlineAlignment(pipelineOptions);

        Injector injector = Guice.createInjector(new NanostreamCannabisModule.Builder()
                .setFromOptions(pipelineOptions)
                .build());

        NameProvider nameProvider = injector.getInstance(NameProvider.class);
        String currentTimeInDefaultFormat = nameProvider.getCurrentTimeInDefaultFormat();
        pipelineOptions.setJobName(nameProvider.buildJobName(JOB_NAME_PREFIX, pipelineOptions.getSraSamplesToFilter()));

        final String stagedBucket = injector.getInstance(Key.get(String.class, Names.named("resultBucket")));
        final String outputDir = injector.getInstance(Key.get(String.class, Names.named("outputDir")));

        final String stagedDir = outputDir + "staged/";

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        pipeline
                .apply(Create.of(BlobId.of(stagedBucket, stagedDir + "result_dv/")))
                .apply(ParDo.of(new DoFn<BlobId, KV<ReferenceDatabase, String>>() {

                    private final static int BATCH_SIZE = 50;

                    private GCSService gcsService;

                    @Setup
                    public void setUp() {
                        gcsService = GCSService.initialize(new FileUtils());
                    }

                    private void processCopyAndOutput(ProcessContext c, String vcfToBqWorkDir, String reference, List<BlobId> blobs, int index) {
                        BlobId destDir = BlobId.of(stagedBucket, String.format(vcfToBqWorkDir + "temp/%s/%s_%d/",
                                currentTimeInDefaultFormat, reference, index));
                        blobs.forEach(blobId -> gcsService.copy(blobId, BlobId.of(destDir.getBucket(), destDir.getName() + new FileUtils().getFilenameFromPath(blobId.getName()))));
                        c.output(KV.of(new ReferenceDatabase(reference, new ArrayList<>()), gcsService.getUriFromBlob(destDir) + "*"));
                    }

                    @ProcessElement
                    public void processElement(ProcessContext c) {

                        String vcfToBqWorkDir = stagedDir + "vcf_to_bq/";
                        BlobId vcfToBqProcessedBlobId = BlobId.of(stagedBucket, vcfToBqWorkDir + "vcf_to_bq_processed.csv");

                        if (!gcsService.isExists(vcfToBqProcessedBlobId)) {
                            gcsService.saveContentToGcs(vcfToBqProcessedBlobId.getBucket(), vcfToBqProcessedBlobId.getName(), "".getBytes());
                        }

                        try {
                            String vcfToBqProcessedData = gcsService.readBlob(new IoUtils(), vcfToBqProcessedBlobId.getBucket(),
                                    vcfToBqProcessedBlobId.getName());
                            List<String> processedFiles = Arrays.asList(vcfToBqProcessedData.split("\n")).stream().map(name -> {
                                String[] split = name.split(",");
                                return split;
                            })
                                    .filter(split -> split.length > 1)
                                    .map(split -> String.format("%s_%s.vcf", split[0], split[1]))
                                    .collect(Collectors.toList());

                            BlobId element = c.element();

                            List<BlobId> blobsToProcess = gcsService.getAllBlobsIn(element.getBucket(), element.getName())
                                    .stream()
                                    .map(blob -> blob.getBlobId())
                                    .filter(blobId -> {
                                        String filenameFromPath = new FileUtils().getFilenameFromPath(blobId.getName());
                                        return !processedFiles.contains(filenameFromPath);
                                    })
                                    .collect(Collectors.toList());
                            Map<String, List<BlobId>> processMap = new HashMap<>();

                            FileUtils fileUtils = new FileUtils();
                            blobsToProcess.forEach(blobId -> {
                                String name = fileUtils.changeFileExtension(fileUtils.getFilenameFromPath(blobId.getName()), "");
                                String[] nameParts = name.split("_");
                                String reference = nameParts[1];

                                if (!processMap.containsKey(reference)) {
                                    processMap.put(reference, new ArrayList<>());
                                }
                                processMap.get(reference).add(blobId);

                                processMap.entrySet().forEach(stringListEntry -> {
                                    int currentSize = stringListEntry.getValue().size();
                                    if (stringListEntry.getValue().size() % BATCH_SIZE == 0) {
                                        processCopyAndOutput(c, vcfToBqWorkDir, stringListEntry.getKey(),
                                                blobsToProcess.subList(currentSize - BATCH_SIZE, currentSize), currentSize);
                                    }
                                });
                            });

                            processMap.entrySet().forEach(stringListEntry -> {
                                int currentSize = stringListEntry.getValue().size();
                                int lastBatchSize = currentSize % BATCH_SIZE;
                                processCopyAndOutput(c, vcfToBqWorkDir, stringListEntry.getKey(),
                                        blobsToProcess.subList(currentSize - lastBatchSize, currentSize), currentSize);

                            });
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }))
                .apply(ParDo.of(injector.getInstance(VcfToBqFn.class)))
                .apply(ParDo.of(new DoFn<KV<ReferenceDatabase, String>, BlobId>() {

                    private GCSService gcsService;

                    @Setup
                    public void setUp() {
                        gcsService = GCSService.initialize(new FileUtils());
                    }

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<ReferenceDatabase, String> element = c.element();
                        String path = element.getValue().replace("*", "");
                        BlobId gcsServiceBlobIdFromUri = gcsService.getBlobIdFromUri(path);
                        LOG.info(path);

                        List<Blob> allBlobsIn = gcsService.getAllBlobsIn(gcsServiceBlobIdFromUri.getBucket(), gcsServiceBlobIdFromUri.getName());

                        String vcfToBqWorkDir = stagedDir + "vcf_to_bq/";
                        BlobId vcfToBqProcessedBlobId = BlobId.of(stagedBucket, vcfToBqWorkDir + "vcf_to_bq_processed.csv");
                        try {
                            String vcfToBqProcessedData = gcsService.readBlob(new IoUtils(), vcfToBqProcessedBlobId.getBucket(),
                                    vcfToBqProcessedBlobId.getName());

                            StringBuilder stringBuilder = new StringBuilder(vcfToBqProcessedData);
                            allBlobsIn.stream().forEach(blob -> {
                                System.out.println(blob.getBlobId());
                                String[] parts = new FileUtils().getFilenameFromPath(blob.getBlobId().getName()).split("\\.")[0].split("_");
                                stringBuilder.append(parts[0]).append(",").append(parts[1]).append("\n");
                            });
                            gcsService.saveContentToGcs(vcfToBqProcessedBlobId.getBucket(), vcfToBqProcessedBlobId.getName(),
                                    stringBuilder.toString().getBytes());

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }))

        ;

        PipelineResult run = pipeline.run();
        if (pipelineOptions.getRunner().getName().equals(DirectRunner.class.getName())) {
            run.waitUntilFinish();
        }
    }
}
