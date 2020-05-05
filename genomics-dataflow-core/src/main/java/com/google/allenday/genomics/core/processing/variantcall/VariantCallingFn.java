package com.google.allenday.genomics.core.processing.variantcall;

import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.SamRecordsChunkMetadataKey;
import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.allenday.genomics.core.reference.ReferenceProvider;
import com.google.allenday.genomics.core.gcp.ResourceProvider;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Apache Beam DoFn function,
 * that provides <a href="https://www.ebi.ac.uk/training/online/course/human-genetic-variation-i-introduction-2019/variant-identification-and-analysis">Variant Calling</a> logic.
 */
public class VariantCallingFn extends DoFn<KV<SamRecordsChunkMetadataKey, KV<ReferenceDatabaseSource, BamWithIndexUris>>, KV<SamRecordsChunkMetadataKey, KV<String, String>>> {

    private Logger LOG = LoggerFactory.getLogger(VariantCallingFn.class);

    private VariantCallingService variantCallingService;
    private String gcsOutputDir;
    private String outputBucketName;
    private ResourceProvider resourceProvider;
    private FileUtils fileUtils;
    private GcsService gcsService;
    private ReferenceProvider referencesProvider;

    public VariantCallingFn(VariantCallingService variantCallingService, FileUtils fileUtils, ReferenceProvider referencesProvider,
                            String outputBucketName, String gcsOutputDir) {
        this.variantCallingService = variantCallingService;
        this.gcsOutputDir = gcsOutputDir;
        this.fileUtils = fileUtils;
        this.outputBucketName = outputBucketName;
        this.referencesProvider = referencesProvider;
    }

    @Setup
    public void setUp() {
        resourceProvider = ResourceProvider.initialize();
        gcsService = GcsService.initialize(fileUtils);
        variantCallingService.setup();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of Variant Calling: %s", c.element().toString()));

        KV<SamRecordsChunkMetadataKey, KV<ReferenceDatabaseSource, BamWithIndexUris>> input = c.element();
        SamRecordsChunkMetadataKey samRecordsChunkMetadataKey = input.getKey();
        KV<ReferenceDatabaseSource, BamWithIndexUris> dbAndBamWithIndexUris = input.getValue();
        ReferenceDatabaseSource referenceDatabaseSource = dbAndBamWithIndexUris.getKey();
        BamWithIndexUris bamWithIndexUris = dbAndBamWithIndexUris.getValue();


        if (samRecordsChunkMetadataKey == null || bamWithIndexUris == null || referenceDatabaseSource == null) {
            LOG.error("Data error");
            LOG.error("referenceDatabase: " + referenceDatabaseSource);
            LOG.error("samRecordsChunkMetadataKey: " + samRecordsChunkMetadataKey);
            LOG.error("bamWithIndexUris: " + bamWithIndexUris);
            return;
        }
        try {
            ReferenceDatabase referenceDd = referencesProvider.getReferenceDd(gcsService, referenceDatabaseSource);

            String outputNameWithoutExt = samRecordsChunkMetadataKey.generateSlug();

            String dirPrefix = samRecordsChunkMetadataKey.getSraSampleId().getValue() + "_" + referenceDd.getDbName();
            String dvGcsOutputDir = gcsService.getUriFromBlob(BlobId.of(outputBucketName, gcsOutputDir + dirPrefix + "/"));

            Triplet<String, Boolean, String> result = variantCallingService.processSampleWithVariantCaller(
                    resourceProvider,
                    dvGcsOutputDir,
                    outputNameWithoutExt,
                    bamWithIndexUris.getBamUri(),
                    bamWithIndexUris.getIndexUri(),
                    samRecordsChunkMetadataKey.generatRegionsValue(),
                    referenceDd,
                    samRecordsChunkMetadataKey.getSraSampleId().getValue());

            if (result.getValue1()) {
                c.output(KV.of(input.getKey(), KV.of(result.getValue0(), dvGcsOutputDir)));
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }
}
