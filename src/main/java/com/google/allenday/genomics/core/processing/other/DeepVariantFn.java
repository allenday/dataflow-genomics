package com.google.allenday.genomics.core.processing.other;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.GeneReadGroupMetaData;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.processing.DeepVariantService;
import com.google.allenday.genomics.core.utils.ResourceProvider;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeepVariantFn extends DoFn<KV<KV<GeneReadGroupMetaData, ReferenceDatabase>, BamWithIndexUris>, KV<KV<GeneReadGroupMetaData, ReferenceDatabase>, String>> {

    private Logger LOG = LoggerFactory.getLogger(DeepVariantFn.class);

    private DeepVariantService deepVariantService;
    private String gcsOutputDir;
    private String outputBucketName;
    private ResourceProvider resourceProvider;
    private FileUtils fileUtils;
    private GCSService gcsService;

    public DeepVariantFn(DeepVariantService deepVariantService, FileUtils fileUtils, String outputBucketName, String gcsOutputDir) {
        this.deepVariantService = deepVariantService;
        this.gcsOutputDir = gcsOutputDir;
        this.fileUtils = fileUtils;
        this.outputBucketName = outputBucketName;
    }

    @Setup
    public void setUp() {
        resourceProvider = ResourceProvider.initialize();
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of Deep Variant: %s", c.element().toString()));

        KV<KV<GeneReadGroupMetaData, ReferenceDatabase>, BamWithIndexUris> input = c.element();
        ReferenceDatabase referenceDatabase = input.getKey().getValue();
        GeneReadGroupMetaData geneReadGroupMetaData = input.getKey().getKey();
        BamWithIndexUris bamWithIndexUris = input.getValue();

        if (geneReadGroupMetaData == null || bamWithIndexUris == null || referenceDatabase == null) {
            LOG.error("Data error");
            LOG.error("referenceDatabase: " + referenceDatabase);
            LOG.error("geneReadGroupMetaData: " + geneReadGroupMetaData);
            LOG.error("bamWithIndexUris: " + bamWithIndexUris);
            return;
        }
        String readGroupAndDb = geneReadGroupMetaData.getSraSample() + "_" + referenceDatabase.getDbName();
        String dvGcsOutputDir = gcsService.getUriFromBlob(BlobId.of(outputBucketName, gcsOutputDir + readGroupAndDb + "/"));

        Triplet<String, Boolean, String> result = deepVariantService.processExampleWithDeepVariant(resourceProvider,
                dvGcsOutputDir, readGroupAndDb, bamWithIndexUris.getBamUri(), bamWithIndexUris.getIndexUri(), referenceDatabase,
                geneReadGroupMetaData.getSraSample());

        if (result.getValue1()) {
            c.output(KV.of(input.getKey(), result.getValue0()));
        }
    }
}
