package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.allenday.genomics.core.model.SraSampleIdReferencePair;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;

public class PrepareDvNotProcessedFn extends DoFn<KV<SraSampleId, Iterable<SampleMetaData>>, KV<SraSampleIdReferencePair, BamWithIndexUris>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareDvNotProcessedFn.class);

    private ReferencesProvider referencesProvider;
    private List<String> references;
    private int minThresholdMb;
    private int maxThresholdMb;
    private String stagedBucket;
    private String mergedFilePattern;
    private String indexFilePattern;
    private String vcfFilePattern;

    private GCSService gcsService;

    public PrepareDvNotProcessedFn(ReferencesProvider referencesProvider, List<String> references, int minThresholdMb,
                                   int maxThresholdMb, String stagedBucket,
                                   String mergedFilePattern, String indexFilePattern, String vcfFilePattern) {
        this.referencesProvider = referencesProvider;
        this.references = references;
        this.minThresholdMb = minThresholdMb;
        this.maxThresholdMb = maxThresholdMb;
        this.stagedBucket = stagedBucket;
        this.mergedFilePattern = mergedFilePattern;
        this.indexFilePattern = indexFilePattern;
        this.vcfFilePattern = vcfFilePattern;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(new FileUtils());
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SraSampleId, Iterable<SampleMetaData>> input = c.element();

        @Nonnull
        SraSampleId sraSampleId = input.getKey();

        for (String ref : references) {
            BlobId blobIdMerge = BlobId.of(stagedBucket, String.format(mergedFilePattern, sraSampleId.getValue(), ref));
            BlobId blobIdIndex = BlobId.of(stagedBucket, String.format(indexFilePattern, sraSampleId.getValue(), ref));
            BamWithIndexUris bamWithIndexUris = new BamWithIndexUris(gcsService.getUriFromBlob(blobIdMerge), gcsService.getUriFromBlob(blobIdIndex));
            BlobId blobIdDv = BlobId.of(stagedBucket, String.format(vcfFilePattern, sraSampleId.getValue(), ref));

            boolean existsDv = gcsService.isExists(blobIdDv);
            boolean mergeExists = gcsService.isExists(blobIdMerge);
            boolean indexExists = gcsService.isExists(blobIdIndex);


            if (mergeExists && indexExists && !existsDv) {
                float sizeMb = gcsService.getBlobSize(blobIdMerge) / (float) (1024 * 1024);
                if (sizeMb >= minThresholdMb && sizeMb <= maxThresholdMb) {
                    LOG.info(String.format("Pass to processing stage: %s", sraSampleId.getValue()));
                    c.output(KV.of(new SraSampleIdReferencePair(sraSampleId, referencesProvider.getReferenceDd(gcsService, ref)),
                            bamWithIndexUris));
                }
            }

        }
    }
}