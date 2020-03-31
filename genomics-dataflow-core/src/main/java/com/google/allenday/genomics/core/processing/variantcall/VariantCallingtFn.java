package com.google.allenday.genomics.core.processing.variantcall;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.allenday.genomics.core.model.SraSampleIdReferencePair;
import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.allenday.genomics.core.reference.ReferenceProvider;
import com.google.allenday.genomics.core.utils.ResourceProvider;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Beam DoFn function,
 * that provides <a href="https://www.ebi.ac.uk/training/online/course/human-genetic-variation-i-introduction-2019/variant-identification-and-analysis">Variant Calling</a> logic.
 * Currently supported <a href="https://github.com/google/deepvariant>Deep Varian</a> variant caller pipeline from Google.
 */
public class VariantCallingtFn extends DoFn<KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, BamWithIndexUris>>, KV<SraSampleIdReferencePair, String>> {

    private Logger LOG = LoggerFactory.getLogger(VariantCallingtFn.class);

    private VariantCallingService variantCallingService;
    private String gcsOutputDir;
    private String outputBucketName;
    private ResourceProvider resourceProvider;
    private FileUtils fileUtils;
    private GCSService gcsService;
    private ReferenceProvider referencesProvider;

    public VariantCallingtFn(VariantCallingService variantCallingService, FileUtils fileUtils, ReferenceProvider referencesProvider,
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
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of Deep Variant: %s", c.element().toString()));

        KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, BamWithIndexUris>> input = c.element();
        SraSampleId sraSampleId = input.getKey().getSraSampleId();
        KV<ReferenceDatabaseSource, BamWithIndexUris> dbAndBamWithIndexUris = input.getValue();
        ReferenceDatabaseSource referenceDatabaseSource = dbAndBamWithIndexUris.getKey();
        BamWithIndexUris bamWithIndexUris = dbAndBamWithIndexUris.getValue();


        if (sraSampleId == null || bamWithIndexUris == null || referenceDatabaseSource == null) {
            LOG.error("Data error");
            LOG.error("referenceDatabase: " + referenceDatabaseSource);
            LOG.error("geneReadGroupMetaData: " + sraSampleId);
            LOG.error("bamWithIndexUris: " + bamWithIndexUris);
            return;
        }
        ReferenceDatabase referenceDd = referencesProvider.getReferenceDd(gcsService, referenceDatabaseSource);

        String readGroupAndDb = sraSampleId + "_" + referenceDatabaseSource.getName();
        String dvGcsOutputDir = gcsService.getUriFromBlob(BlobId.of(outputBucketName, gcsOutputDir + readGroupAndDb + "/"));

        Triplet<String, Boolean, String> result = variantCallingService.processSampleWithVariantCaller(resourceProvider,
                dvGcsOutputDir, readGroupAndDb, bamWithIndexUris.getBamUri(), bamWithIndexUris.getIndexUri(), referenceDd,
                sraSampleId.getValue());

        if (result.getValue1()) {
            c.output(KV.of(input.getKey(), result.getValue0()));
        }
    }
}
