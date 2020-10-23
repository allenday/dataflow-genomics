package com.google.allenday.genomics.core.pipeline.batch.partsprocessing;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.export.vcftobq.VcfToBqFn;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class VcfToBqBatchTransform extends PTransform<PCollection<BlobId>,
        PCollection<BlobId>> {
    Logger LOG = LoggerFactory.getLogger(VcfToBqBatchTransform.class);

    private PrepareVcfToBqBatchFn prepareVcfToBqBatchFn;
    private SaveVcfToBqResults saveVcfToBqResults;
    private VcfToBqFn vcfToBqFn;

    public VcfToBqBatchTransform(PrepareVcfToBqBatchFn prepareVcfToBqBatchFn, SaveVcfToBqResults saveVcfToBqResults,
                                 VcfToBqFn vcfToBqFn) {
        this.prepareVcfToBqBatchFn = prepareVcfToBqBatchFn;
        this.saveVcfToBqResults = saveVcfToBqResults;
        this.vcfToBqFn = vcfToBqFn;
    }

    @Override
    public PCollection<BlobId> expand(PCollection<BlobId> input) {
        return input.apply("Prepare for export", ParDo.of(prepareVcfToBqBatchFn))
                .apply("Export VCF files to BQ", ParDo.of(vcfToBqFn))
                .apply("Save VCF to BQ processed samples", ParDo.of(saveVcfToBqResults))
                ;
    }


    public static class PrepareVcfToBqBatchFn extends DoFn<BlobId, KV<String, String>> {

        private final static int DEFAULT_BATCH_SIZE = 50;

        private final String BATCH_VCF_TO_BQ_TEMP_FILES_PATTERN = "temp/%s_%d/";

        private GcsService gcsService;

        private FileUtils fileUtils;
        private StagingPathsBulder stagingPathsBulder;
        private String jobTime;
        private String vcfToBqOutputDir;
        private int batchSize;

        public PrepareVcfToBqBatchFn(FileUtils fileUtils,
                                     StagingPathsBulder stagingPathsBulder,
                                     String vcfToBqOutputDir,
                                     String jobTime) {
            this(fileUtils, stagingPathsBulder, jobTime, vcfToBqOutputDir, DEFAULT_BATCH_SIZE);
        }

        public PrepareVcfToBqBatchFn(FileUtils fileUtils, StagingPathsBulder stagingPathsBulder, String jobTime,
                                     String vcfToBqOutputDir, int batchSize) {
            this.fileUtils = fileUtils;
            this.stagingPathsBulder = stagingPathsBulder;
            this.jobTime = jobTime;
            this.vcfToBqOutputDir = vcfToBqOutputDir;
            this.batchSize = batchSize;
        }

        @Setup
        public void setUp() {
            gcsService = GcsService.initialize(fileUtils);
        }

        private void processCopyAndOutput(ProcessContext c, String reference, List<BlobId> blobs, int index) {
            BlobId destDir = BlobId.of(stagingPathsBulder.getStagingBucket(),
                    vcfToBqOutputDir + String.format(BATCH_VCF_TO_BQ_TEMP_FILES_PATTERN, reference, index));
            blobs.forEach(blobId -> gcsService.copy(blobId, BlobId.of(destDir.getBucket(),
                    destDir.getName() + new FileUtils().getFilenameFromPath(blobId.getName()))));
            c.output(KV.of(reference, gcsService.getUriFromBlob(destDir) + "*"));
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            BlobId vcfToBqProcessedBlobId = stagingPathsBulder.getVcfToBqProcessedListFileBlobId();
            if (!gcsService.isExists(vcfToBqProcessedBlobId)) {
                gcsService.saveContentToGcs(vcfToBqProcessedBlobId, "".getBytes());
            }

            try {
                String vcfToBqProcessedData = gcsService.readBlob(vcfToBqProcessedBlobId);
                List<String> processedFiles = Arrays.stream(vcfToBqProcessedData.split("\n")).map(name -> name.split(","))
                        .filter(split -> split.length > 1)
                        .map(split -> String.format("%s_%s.vcf", split[0], split[1]))
                        .collect(Collectors.toList());

                BlobId element = c.element();

                List<BlobId> blobsToProcess =
                        StreamSupport.stream(gcsService.getBlobsWithPrefix(element.getBucket(), element.getName()).spliterator(), false)
                                .map(BlobInfo::getBlobId)
                                .filter(blobId -> {
                                    String filenameFromPath = new FileUtils().getFilenameFromPath(blobId.getName());
                                    return !processedFiles.contains(filenameFromPath);
                                })
                                .collect(Collectors.toList());
                Map<String, List<BlobId>> processMap = new HashMap<>();

                blobsToProcess.forEach(blobId -> {
                    String name = fileUtils.changeFileExtension(fileUtils.getFilenameFromPath(blobId.getName()), "");
                    int divider = name.indexOf("_");
                    String reference = name.substring(divider + 1);

                    if (!processMap.containsKey(reference)) {
                        processMap.put(reference, new ArrayList<>());
                    }
                    processMap.get(reference).add(blobId);
                    int currentSize = processMap.get(reference).size();
                    if (processMap.get(reference).size() % batchSize == 0) {
                        processCopyAndOutput(c, reference, new ArrayList<>(processMap.get(reference).subList(currentSize - batchSize, currentSize)), currentSize);
                    }
                });

                processMap.forEach((key, value) -> {
                    int currentSize = value.size();
                    int lastBatchSize = currentSize % batchSize;
                    processCopyAndOutput(c, key, new ArrayList<>(value.subList(currentSize - lastBatchSize, currentSize)), currentSize);

                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class SaveVcfToBqResults extends DoFn<KV<String, String>, BlobId> {
        Logger LOG = LoggerFactory.getLogger(SaveVcfToBqResults.class);

        private GcsService gcsService;

        private StagingPathsBulder stagingPathsBulder;
        private FileUtils fileUtils;

        public SaveVcfToBqResults(StagingPathsBulder stagingPathsBulder,
                                  FileUtils fileUtils) {
            this.stagingPathsBulder = stagingPathsBulder;
            this.fileUtils = fileUtils;
        }

        @Setup
        public void setUp() {
            gcsService = GcsService.initialize(new FileUtils());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, String> element = c.element();
            String path = element.getValue().replace("*", "");
            BlobId gcsServiceBlobIdFromUri = gcsService.getBlobIdFromUri(path);

            List<Blob> allBlobsIn = StreamSupport.stream(
                    gcsService.getBlobsWithPrefix(gcsServiceBlobIdFromUri.getBucket(), gcsServiceBlobIdFromUri.getName())
                            .spliterator(), false)
                    .collect(Collectors.toList());

            try {
                String vcfToBqProcessedData = gcsService.readBlob(stagingPathsBulder.getVcfToBqProcessedListFileBlobId());

                StringBuilder stringBuilder = new StringBuilder(vcfToBqProcessedData);
                allBlobsIn.forEach(blob -> {
                    LOG.info(String.format("BlobId: %s", blob.getBlobId()));

                    String name = fileUtils.changeFileExtension(fileUtils.getFilenameFromPath(blob.getBlobId().getName()), "");
                    int divider = name.indexOf("_");
                    String reference = name.substring(divider + 1);
                    String sampleName = name.substring(0, divider);

                    stringBuilder.append(sampleName).append(",").append(reference).append("\n");
                });
                gcsService.saveContentToGcs(stagingPathsBulder.getVcfToBqProcessedListFileBlobId(),
                        stringBuilder.toString().getBytes());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}