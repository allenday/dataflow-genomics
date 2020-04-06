package com.google.allenday.genomics.core.example;

import com.google.allenday.genomics.core.batch.BatchProcessingPipelineOptions;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.pipeline.PipelineSetupUtils;
import com.google.allenday.genomics.core.processing.AlignAndPostProcessTransform;
import com.google.allenday.genomics.core.processing.SplitFastqIntoBatches;
import com.google.allenday.genomics.core.processing.variantcall.VariantCallingTransform;
import com.google.allenday.genomics.core.processing.vcf_to_bq.PrepareAndExecuteVcfToBqTransform;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Main class of genomics-dataflow-core usage with GIAB example
 */
public class GiabExampleApp {
    private final static String JOB_NAME_PREFIX = "giab-example-processing-";

    public static void main(String[] args) {
        BatchProcessingPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(BatchProcessingPipelineOptions.class);
        PipelineSetupUtils.prepareForInlineAlignment(pipelineOptions);

        Injector injector = Guice.createInjector(new GiabExampleAppModule(pipelineOptions));

        NameProvider nameProvider = injector.getInstance(NameProvider.class);
        pipelineOptions.setJobName(nameProvider.buildJobName(JOB_NAME_PREFIX, pipelineOptions.getSraSamplesToFilter()));

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        pipeline
                .apply("Parse data", injector.getInstance(ParseSourceCsvTransform.class))
                .apply("Split large FASTQ into chunks", injector.getInstance(SplitFastqIntoBatches.class))
                .apply("Align reads and prepare for DV", injector.getInstance(AlignAndPostProcessTransform.class))
                .apply("Variant Calling", injector.getInstance(VariantCallingTransform.class))
                .apply("Prepare and execute export to BigQuery", injector.getInstance(PrepareAndExecuteVcfToBqTransform.class))
        ;

        pipeline.run();
    }
}
