package com.google.allenday.genomics.core.pipeline;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public class PipelineSetupUtils {

    public static void prepareForInlineAlignment(DataflowPipelineOptions dataflowPipelineOptions) {
        dataflowPipelineOptions.setNumberOfWorkerHarnessThreads(1);
    }
}
