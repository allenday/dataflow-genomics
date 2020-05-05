package com.google.allenday.genomics.core.pipeline.batch.partsprocessing;

import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.SamRecordsChunkMetadataKey;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;

public class PrepareDvNotProcessedFn extends DoFn<KV<SraSampleId, Iterable<SampleRunMetaData>>,
        KV<SamRecordsChunkMetadataKey, KV<ReferenceDatabaseSource, BamWithIndexUris>>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareDvNotProcessedFn.class);

    private List<String> references;
    private int minThresholdMb;
    private int maxThresholdMb;
    private StagingPathsBulder stagingPathsBulder;
    private String allReferencesDirGcsUri;

    private GcsService gcsService;

    public PrepareDvNotProcessedFn(List<String> references, int minThresholdMb,
                                   int maxThresholdMb, StagingPathsBulder stagingPathsBulder, String allReferencesDirGcsUri) {
        this.references = references;
        this.minThresholdMb = minThresholdMb;
        this.maxThresholdMb = maxThresholdMb;
        this.stagingPathsBulder = stagingPathsBulder;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
    }

    @Setup
    public void setUp() {
        gcsService = GcsService.initialize(new FileUtils());
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SraSampleId, Iterable<SampleRunMetaData>> input = c.element();

        @Nonnull
        SraSampleId sraSampleId = input.getKey();

        for (String ref : references) {
            BlobId blobIdMerge = stagingPathsBulder.buildMergedBlobId(sraSampleId.getValue(), ref);
            BlobId blobIdIndex = stagingPathsBulder.buildIndexBlobId(sraSampleId.getValue(), ref);
            BamWithIndexUris bamWithIndexUris = new BamWithIndexUris(gcsService.getUriFromBlob(blobIdMerge), gcsService.getUriFromBlob(blobIdIndex));
            BlobId blobIdDv = stagingPathsBulder.buildVcfFileBlobId(sraSampleId.getValue(), ref);

            boolean existsDv = gcsService.isExists(blobIdDv);
            boolean mergeExists = gcsService.isExists(blobIdMerge);
            boolean indexExists = gcsService.isExists(blobIdIndex);


            if (mergeExists && indexExists && !existsDv) {
                float sizeMb = gcsService.getBlobSize(blobIdMerge) / (float) (1024 * 1024);
                if (sizeMb >= minThresholdMb && sizeMb <= maxThresholdMb) {
                    LOG.info(String.format("Pass to processing stage: %s", sraSampleId.getValue()));

                    ReferenceDatabaseSource referenceDatabaseSource =
                            new ReferenceDatabaseSource.ByNameAndUriSchema(ref, allReferencesDirGcsUri);
                    c.output(KV.of(new SamRecordsChunkMetadataKey(sraSampleId, referenceDatabaseSource.getName()),
                            KV.of(referenceDatabaseSource, bamWithIndexUris)));
                }
            }

        }
    }
}