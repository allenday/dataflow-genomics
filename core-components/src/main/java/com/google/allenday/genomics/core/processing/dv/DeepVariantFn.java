package com.google.allenday.genomics.core.processing.dv;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.allenday.genomics.core.model.SraSampleIdReferencePair;
import com.google.allenday.genomics.core.utils.ResourceProvider;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeepVariantFn extends DoFn<KV<SraSampleIdReferencePair, BamWithIndexUris>, KV<SraSampleIdReferencePair, String>> {

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

        KV<SraSampleIdReferencePair, BamWithIndexUris> input = c.element();
        ReferenceDatabase referenceDatabase = input.getKey().getReferenceDatabase();
        SraSampleId sraSampleId = input.getKey().getSraSampleId();
        BamWithIndexUris bamWithIndexUris = input.getValue();

        if (sraSampleId == null || bamWithIndexUris == null || referenceDatabase == null) {
            LOG.error("Data error");
            LOG.error("referenceDatabase: " + referenceDatabase);
            LOG.error("geneReadGroupMetaData: " + sraSampleId);
            LOG.error("bamWithIndexUris: " + bamWithIndexUris);
            return;
        }
        String readGroupAndDb = sraSampleId + "_" + referenceDatabase.getDbName();
        String dvGcsOutputDir = gcsService.getUriFromBlob(BlobId.of(outputBucketName, gcsOutputDir + readGroupAndDb + "/"));

        Triplet<String, Boolean, String> result = deepVariantService.processSampleWithDeepVariant(resourceProvider,
                dvGcsOutputDir, readGroupAndDb, bamWithIndexUris.getBamUri(), bamWithIndexUris.getIndexUri(), referenceDatabase,
                sraSampleId.getValue());

        if (result.getValue1()) {
            c.output(KV.of(input.getKey(), result.getValue0()));
        }
    }
}
