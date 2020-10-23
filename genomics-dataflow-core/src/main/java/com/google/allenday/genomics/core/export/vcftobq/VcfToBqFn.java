package com.google.allenday.genomics.core.export.vcftobq;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.gcp.ResourceProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Beam DoFn function, that exports Variant Calling results (VCF) into the <a href="https://cloud.google.com/bigquery">BigQuery</a> table.
 * Uses vcf-to-bigquery transform from <a href="https://github.com/googlegenomics/gcp-variant-transforms">GCP Variant Transforms</a>
 */
public class VcfToBqFn extends DoFn<KV<String, String>, KV<String, String>> {

    private Logger LOG = LoggerFactory.getLogger(VcfToBqFn.class);

    private VcfToBqService vcfToBqService;
    private ResourceProvider resourceProvider;
    private FileUtils fileUtils;

    private GcsService gcsService;

    public VcfToBqFn(VcfToBqService vcfToBqService, FileUtils fileUtils) {
        this.vcfToBqService = vcfToBqService;
        this.fileUtils = fileUtils;
    }

    @Setup
    public void setUp() {
        resourceProvider = ResourceProvider.initialize();
        gcsService = GcsService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of Vcf To Bq: %s", c.element().toString()));

        KV<String, String> input = c.element();
        String referenceDatabaseName = input.getKey();
        String vcfUri = input.getValue();

        if (vcfUri == null || referenceDatabaseName == null) {
            LOG.error("Data error");
            LOG.error("vcfUri: " + vcfUri);
            return;
        }
        Pair<Boolean, String> result = vcfToBqService.convertVcfFileToBq(gcsService, resourceProvider,
                fileUtils, referenceDatabaseName, vcfUri);

        if (result.getValue0()) {
            c.output(KV.of(input.getKey(), vcfUri));
        }
    }
}
