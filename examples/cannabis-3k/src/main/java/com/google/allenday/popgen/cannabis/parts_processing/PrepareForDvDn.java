package com.google.allenday.popgen.cannabis.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.ReadGroupMetaData;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PrepareForDvDn extends DoFn<KV<ReadGroupMetaData, Iterable<SampleMetaData>>, KV<KV<ReadGroupMetaData, ReferenceDatabase>, BamWithIndexUris>> {

    private Logger LOG = LoggerFactory.getLogger(NanostreamBatchAppDV.class);

    private ReferencesProvider referencesProvider;
    private List<String> references;
    private int minThresholdMb;
    private int maxThresholdMb;
    private GCSService gcsService;
    private String stagedBucket;
    private String stagedDir;

    public PrepareForDvDn(ReferencesProvider referencesProvider, List<String> references, int minThresholdMb, int maxThresholdMb,
                          String stagedBucket, String stagedDir) {
        this.referencesProvider = referencesProvider;
        this.references = references;
        this.minThresholdMb = minThresholdMb;
        this.maxThresholdMb = maxThresholdMb;
        this.stagedBucket = stagedBucket;
        this.stagedDir = stagedDir;
    }

    @DoFn.Setup
    public void setUp() {
        gcsService = GCSService.initialize(new FileUtils());
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        KV<ReadGroupMetaData, Iterable<SampleMetaData>> input = c.element();
        ReadGroupMetaData geneReadGroupMetaData = input.getKey();

        for (String ref : references) {
            BlobId blobIdMerge = BlobId.of(stagedBucket, String.format(stagedDir + "result_merged_bam/%s_%s.merged.sorted.bam", geneReadGroupMetaData.getSraSample(), ref));
            BlobId blobIdIndex = BlobId.of(stagedBucket, String.format(stagedDir + "result_merged_bam/%s_%s.merged.sorted.bam.bai", geneReadGroupMetaData.getSraSample(), ref));
            BamWithIndexUris bamWithIndexUris = new BamWithIndexUris(gcsService.getUriFromBlob(blobIdMerge), gcsService.getUriFromBlob(blobIdIndex));
            BlobId blobIdDv = BlobId.of(stagedBucket, String.format(stagedDir + "result_dv/%s_%s/%s_%s.vcf", geneReadGroupMetaData.getSraSample(), ref, geneReadGroupMetaData.getSraSample(), ref));

            boolean existsDv = gcsService.isExists(blobIdDv);
            boolean mergeExists = gcsService.isExists(blobIdMerge);
            boolean indexExists = gcsService.isExists(blobIdIndex);


            if (mergeExists && indexExists && !existsDv) {
                float sizeMb = gcsService.getBlobSize(blobIdMerge) / (float) (1024 * 1024);
                if (sizeMb >= minThresholdMb && sizeMb <= maxThresholdMb) {
                    LOG.info(String.format("Pass to processing stage: %s", geneReadGroupMetaData.getSraSample()));
                    c.output(KV.of(KV.of(geneReadGroupMetaData, referencesProvider.getReferenceDd(gcsService, ref)), bamWithIndexUris));
                }
            }

        }
    }
}