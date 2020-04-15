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
                    String.format(stagingPathsBulder.buildVcfToBqDirPath() + "temp/%s/%s_%d/", jobTime, reference, index));
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
                    if (processMap.get(reference).size() % BATCH_SIZE == 0) {
                        processCopyAndOutput(c, reference, new ArrayList<>(processMap.get(reference).subList(currentSize - BATCH_SIZE, currentSize)), currentSize);
                    }
                });

                processMap.forEach((key, value) -> {
                    int currentSize = value.size();
                    int lastBatchSize = currentSize % BATCH_SIZE;
                    processCopyAndOutput(c, key, new ArrayList<>(value.subList(currentSize - lastBatchSize, currentSize)), currentSize);

                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class SaveVcfToBqResults extends DoFn<KV<String, String>, BlobId> {
        Logger LOG = LoggerFactory.getLogger(SaveVcfToBqResults.class);

        private GCSService gcsService;

        private StagingPathsBulder stagingPathsBulder;
        private IoUtils ioUtils;
        private FileUtils fileUtils;

        public SaveVcfToBqResults(StagingPathsBulder stagingPathsBulder,
                                  IoUtils ioUtils, FileUtils fileUtils) {
            this.stagingPathsBulder = stagingPathsBulder;
            this.ioUtils = ioUtils;
            this.fileUtils = fileUtils;
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

            List<Blob> allBlobsIn = StreamSupport.stream(
                    gcsService.getBlobsWithPrefix(gcsServiceBlobIdFromUri.getBucket(), gcsServiceBlobIdFromUri.getName())
                            .spliterator(), false)
                    .collect(Collectors.toList());

            try {
                String vcfToBqProcessedData = gcsService.readBlob(stagingPathsBulder.getVcfToBqProcessedListFileBlobId(), ioUtils);

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