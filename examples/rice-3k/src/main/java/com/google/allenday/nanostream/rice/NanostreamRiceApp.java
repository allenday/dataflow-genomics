package com.google.allenday.nanostream.rice;

import com.google.allenday.genomics.core.batch.BatchProcessingPipelineOptions;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.model.BamWithIndexUris;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.model.SraSampleIdReferencePair;
import com.google.allenday.genomics.core.pipeline.PipelineSetupUtils;
import com.google.allenday.genomics.core.processing.AlignAndPostProcessTransform;
import com.google.allenday.genomics.core.processing.dv.DeepVariantFn;
import com.google.allenday.genomics.core.processing.vcf_to_bq.VcfToBqFn;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.allenday.nanostream.rice.vcf_to_bq.DvAndVcfToBqConnector;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class NanostreamRiceApp {

    private final static String JOB_NAME_PREFIX = "nanostream-rice--";

    public static void main(String[] args) {

        BatchProcessingPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(BatchProcessingPipelineOptions.class);
        PipelineSetupUtils.prepareForInlineAlignment(pipelineOptions);

        Injector injector = Guice.createInjector(new NanostreamRiceModule.Builder()
                .setFromOptions(pipelineOptions)
                .build());

        NameProvider nameProvider = injector.getInstance(NameProvider.class);
        pipelineOptions.setJobName(nameProvider.buildJobName(JOB_NAME_PREFIX, pipelineOptions.getSraSamplesToFilter()));

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<KV<SraSampleIdReferencePair, BamWithIndexUris>> bamWithIndexUris = pipeline
                .apply("Parse data", injector.getInstance(ParseSourceCsvTransform.class))
                .apply("Align reads and prepare for DV", injector.getInstance(AlignAndPostProcessTransform.class));

        if (pipelineOptions.getWithVariantCalling()) {
            PCollection<KV<ReferenceDatabase, String>> vcfResults = bamWithIndexUris
                    .apply("Variant Calling", ParDo.of(injector.getInstance(DeepVariantFn.class)))
                    .apply("Prepare to VcfToBq transform", MapElements.via(new DvAndVcfToBqConnector()));

            if (pipelineOptions.getWithExportVcfToBq()) {
                vcfResults
                        .apply("Export to BigQuery", ParDo.of(injector.getInstance(VcfToBqFn.class)));
            }
        }
        pipeline.run();
    }
}
