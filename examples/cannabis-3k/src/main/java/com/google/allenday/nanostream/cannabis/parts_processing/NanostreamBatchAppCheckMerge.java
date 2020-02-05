package com.google.allenday.nanostream.cannabis.parts_processing;

import com.google.allenday.genomics.core.batch.BatchProcessingPipelineOptions;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.parts_processing.RemoveFailedMergeBamsFn;
import com.google.allenday.genomics.core.parts_processing.StagingPaths;
import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
import com.google.allenday.genomics.core.pipeline.PipelineSetupUtils;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.allenday.nanostream.cannabis.NanostreamCannabisModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;

import java.util.List;

public class NanostreamBatchAppCheckMerge {

    private final static String JOB_NAME_PREFIX = "nanostream-cannabis-check-merge--";


    public static void main(String[] args) {

        BatchProcessingPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(BatchProcessingPipelineOptions.class);
        PipelineSetupUtils.prepareForInlineAlignment(pipelineOptions);

        Injector injector = Guice.createInjector(new NanostreamCannabisModule.Builder()
                .setFromOptions(pipelineOptions)
                .build());

        NameProvider nameProvider = injector.getInstance(NameProvider.class);
        pipelineOptions.setJobName(nameProvider.buildJobName(JOB_NAME_PREFIX, pipelineOptions.getSraSamplesToFilter()));

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        final String stagedBucket = injector.getInstance(Key.get(String.class, Names.named("resultBucket")));
        final String outputDir = injector.getInstance(Key.get(String.class, Names.named("outputDir")));
        StagingPaths stagingPaths = StagingPaths.init(outputDir + "staged/");

        GenomicsOptions genomicsOptions = injector.getInstance(GenomicsOptions.class);
        List<String> geneReferences = genomicsOptions.getGeneReferences();

        pipeline

                .apply("Parse data", injector.getInstance(ParseSourceCsvTransform.class))
                .apply(ParDo.of(new RemoveFailedMergeBamsFn(injector.getInstance(FileUtils.class),
                        geneReferences, stagedBucket, stagingPaths.getSortedFilePattern(),
                        stagingPaths.getMergedFilePattern(), true)))
                .apply(ToString.elements());

        PipelineResult run = pipeline.run();
        if (pipelineOptions.getRunner().getName().equals(DirectRunner.class.getName())) {
            run.waitUntilFinish();
        }
    }
}
