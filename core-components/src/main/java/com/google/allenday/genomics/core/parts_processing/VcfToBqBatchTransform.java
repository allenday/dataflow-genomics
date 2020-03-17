package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.IoUtils;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqFn;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.*;
import java.util.stream.Collectors;

public class VcfToBqBatchTransform extends PTransform<PCollection<BlobId>,
        PCollection<BlobId>> {

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
                .apply("Save VCF to BQ processed samples", ParDo.of(saveVcfToBqResults));
    }


    public static class PrepareVcfToBqBatchFn extends DoFn<BlobId, KV<String, String>> {

        private final static int BATCH_SIZE = 50;

        private GCSService gcsService;

        private FileUtils fileUtils;
        private IoUtils ioUtils;
        private StagingPathsBulder stagingPathsBulder;
        private String jobTime;

        public PrepareVcfToBqBatchFn(FileUtils fileUtils, IoUtils ioUtils, StagingPathsBulder stagingPathsBulder, String jobTime) {
            this.fileUtils = fileUtils;
            this.ioUtils = ioUtils;
            this.stagingPathsBulder = stagingPathsBulder;
            this.jobTime = jobTime;
        }

        @Setup
        public void setUp() {
            gcsService = GCSService.initialize(fileUtils);
        }

        private void processCopyAndOutput(ProcessContext c, String reference, List<BlobId> blobs, int index) {
            BlobId destDir = BlobId.of(stagingPathsBulder.getStagingBucket(),
                    String.format(stagingPathsBulder.buildVcfDirPath() + "temp/%s/%s_%d/", jobTime, reference, index));
            blobs.forEach(blobId -> gcsService.copy(blobId, BlobId.of(destDir.getBucket(), destDir.getName() + new FileUtils().getFilenameFromPath(blobId.getName()))));
            c.output(KV.of(reference, gcsService.getUriFromBlob(destDir) + "*"));
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            BlobId vcfToBqProcessedBlobId = stagingPathsBulder.getVcfToBqProcessedListFileBlobId();
            if (!gcsService.isExists(vcfToBqProcessedBlobId)) {
                gcsService.saveContentToGcs(vcfToBqProcessedBlobId, "".getBytes());
            }

            try {
                String vcfToBqProcessedData = gcsService.readBlob(vcfToBqProcessedBlobId, ioUtils);
                List<String> processedFiles = Arrays.stream(vcfToBqProcessedData.split("\n")).map(name -> name.split(","))
                        .filter(split -> split.length > 1)
                        .map(split -> String.format("%s_%s.vcf", split[0], split[1]))
                        .collect(Collectors.toList());

                BlobId element = c.element();

                List<BlobId> blobsToProcess = gcsService.getAllBlobsIn(element.getBucket(), element.getName())
                        .stream()
                        .map(BlobInfo::getBlobId)
                        .filter(blobId -> {
                            String filenameFromPath = new FileUtils().getFilenameFromPath(blobId.getName());
                            return !processedFiles.contains(filenameFromPath);
                        })
                        .collect(Collectors.toList());
                Map<String, List<BlobId>> processMap = new HashMap<>();

                blobsToProcess.forEach(blobId -> {
                    String name = fileUtils.changeFileExtension(fileUtils.getFilenameFromPath(blobId.getName()), "");
                    String[] nameParts = name.split("_");
                    String reference = nameParts[1];

                    if (!processMap.containsKey(reference)) {
                        processMap.put(reference, new ArrayList<>());
                    }
                    processMap.get(reference).add(blobId);

                    processMap.forEach((key, value) -> {
                        int currentSize = value.size();
                        if (value.size() % BATCH_SIZE == 0) {
                            processCopyAndOutput(c, key, blobsToProcess.subList(currentSize - BATCH_SIZE, currentSize), currentSize);
                        }
                    });
                });

                processMap.forEach((key, value) -> {
                    int currentSize = value.size();
                    int lastBatchSize = currentSize % BATCH_SIZE;
                    processCopyAndOutput(c, key, blobsToProcess.subList(currentSize - lastBatchSize, currentSize), currentSize);

                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class SaveVcfToBqResults extends DoFn<KV<String, String>, BlobId> {

        private GCSService gcsService;

        private StagingPathsBulder stagingPathsBulder;
        private IoUtils ioUtils;

        public SaveVcfToBqResults(StagingPathsBulder stagingPathsBulder, IoUtils ioUtils) {
            this.stagingPathsBulder = stagingPathsBulder;
            this.ioUtils = ioUtils;
        }

        @Setup
        public void setUp() {
            gcsService = GCSService.initialize(new FileUtils());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, String> element = c.element();
            String path = element.getValue().replace("*", "");
            BlobId gcsServiceBlobIdFromUri = gcsService.getBlobIdFromUri(path);

            List<Blob> allBlobsIn = gcsService.getAllBlobsIn(gcsServiceBlobIdFromUri.getBucket(), gcsServiceBlobIdFromUri.getName());

            try {
                String vcfToBqProcessedData = gcsService.readBlob(stagingPathsBulder.getVcfToBqProcessedListFileBlobId(), ioUtils);

                StringBuilder stringBuilder = new StringBuilder(vcfToBqProcessedData);
                allBlobsIn.forEach(blob -> {
                    System.out.println(blob.getBlobId());
                    String[] parts = new FileUtils().getFilenameFromPath(blob.getBlobId().getName()).split("\\.")[0].split("_");
                    stringBuilder.append(parts[0]).append(",").append(parts[1]).append("\n");
                });
                gcsService.saveContentToGcs(stagingPathsBulder.getVcfToBqProcessedListFileBlobId(),
                        stringBuilder.toString().getBytes());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}