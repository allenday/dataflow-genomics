package com.google.allenday.genomics.core.processing;

import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.pipeline.DeepVariantOptions;
import com.google.allenday.genomics.core.processing.lifesciences.LifeSciencesService;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import com.google.allenday.genomics.core.utils.ResourceProvider;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service that provides access to Google Cloud Lifesciences API due to exevute Deep Variant processing.
 */

public class DeepVariantService implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(DeepVariantService.class);


    private final static String DEEP_VARIANT_RUNNER_IMAGE_URI = "gcr.io/cloud-genomics-pipelines/gcp-deepvariant-runner";

    private final static String DEEP_VARIANT_RUNNER_PATH = "/opt/deepvariant_runner/bin/gcp_deepvariant_runner";
    private final static String DEEP_VARIANT_DOCKER_IMAGE = "gcr.io/deepvariant-docker/deepvariant";
    private final static String DEEP_VARIANT_VERSION = "0.9.0";
    private final static String DEEP_VARIANT_MODEL = "gs://deepvariant/models/DeepVariant/0.9.0/DeepVariant-inception_v3-0.9.0+data-wgs_standard";

    private final static String DEEP_VARIANT_STAGING_DIR_NAME = "deep-variant-staging";

    private final static String DEEP_VARIANT_MACHINE_TYPE = "n1-standard-1";

    private final static String DEEP_VARIANT_RESULT_EXTENSION = ".vcf";


    public enum DeepVariantArguments {
        PROJECT("project"),
        ZONES("zones"),
        DOCKER_IMAGE("docker_image"),
        MODEL("model"),
        STAGING("staging"),
        OUTFILE("outfile"),
        BAM("bam"),
        BAI("bai"),
        REF("ref"),
        REF_BAI("ref_fai"),
        SAMPLE_NAME("sample_name"),
        JOB_NAME_PREFIX("job_name_prefix"),
        OPERATION_LABEL("operation_label"),
        MAKE_EXAMPLES_CORES_PER_WORKER("make_examples_cores_per_worker"),
        MAKE_EXAMPLES_RAM_PER_WORKER("make_examples_ram_per_worker_gb"),
        MAKE_EXAMPLES_DISK_PER_WORKER("make_examples_disk_per_worker_gb"),
        CALL_VARIANTS_CORES_PER_WORKER("call_variants_cores_per_worker"),
        CALL_VARIANTS_RAM_PER_WORKER("call_variants_ram_per_worker_gb"),
        CALL_VARIANTS_DISK_PER_WORKER("call_variants_disk_per_worker_gb"),
        POSTPROCESS_VARINATS_CORES("postprocess_variants_cores"),
        POSTPROCESS_VARINATS_RAM("postprocess_variants_ram_gb"),
        POSTPROCESS_VARINATS_DISK("postprocess_variants_disk_gb"),
        MAKE_EXAMPLES_WORKERS("make_examples_workers"),
        CALL_VARIANTS_WORKERS("call_variants_workers"),
        PREEMPTIBLE("preemptible"),
        MAX_PREEMPTIBLE_TRIES("max_premptible_tries"),
        MAX_NON_PREEMPTIBLE_TRIES("max_non_premptible_tries"),
        SHARDS("shards");
        private final String argName;

        DeepVariantArguments(String argName) {
            this.argName = argName;
        }

        public String getArgForCommand() {
            return "--" + argName;
        }
    }

    private ReferencesProvider referencesProvider;
    private LifeSciencesService lifeSciencesService;
    private DeepVariantOptions deepVariantOptions;

    public DeepVariantService(ReferencesProvider referencesProvider, LifeSciencesService lifeSciencesService, DeepVariantOptions deepVariantOptions) {
        this.referencesProvider = referencesProvider;
        this.lifeSciencesService = lifeSciencesService;
        this.deepVariantOptions = deepVariantOptions;
    }

    public Triplet<String, Boolean, String> processSampleWithDeepVariant(ResourceProvider resourceProvider,
                                                                         String outDirGcsUri, String outFilePrefix,
                                                                         String bamUri, String baiUri,
                                                                         ReferenceDatabase referenceDatabase,
                                                                         String readGroupName) {
        Pair<String, String> refUriWithIndex = referenceDatabase.getRefUriWithIndex(referencesProvider.getReferenceFileExtension());

        String outFileUri = outDirGcsUri + outFilePrefix + DEEP_VARIANT_RESULT_EXTENSION;

        String jobNamePrefix = outFilePrefix.toLowerCase() + "_";
        List<String> actionCommands = buildCommand(resourceProvider, outDirGcsUri, outFileUri, bamUri, baiUri,
                refUriWithIndex.getValue0(), refUriWithIndex.getValue1(), readGroupName, jobNamePrefix);

        Pair<Boolean, String> operationResult = lifeSciencesService.runLifesciencesPipelineWithLogging(actionCommands,
                DEEP_VARIANT_RUNNER_IMAGE_URI, outDirGcsUri, deepVariantOptions.getControlPipelineWorkerRegion(),
                DEEP_VARIANT_MACHINE_TYPE, resourceProvider.getProjectNumber(), outFilePrefix);

        return Triplet.with(outFileUri, operationResult.getValue0(), operationResult.getValue1());
    }


    private List<String> buildCommand(ResourceProvider resourceProvider,
                                      String outDirGcsUri, String outfileGcsUri, String bamUri, String baiUri,
                                      String ref, String refIndex,
                                      String sampleName, String jobNamePrefix) {
        Map<DeepVariantArguments, String> args = new HashMap<>();

        args.put(DeepVariantArguments.PROJECT, resourceProvider.getProjectId());
        args.put(DeepVariantArguments.ZONES, deepVariantOptions.getStepsWorkerRegion() + "-*");
        args.put(DeepVariantArguments.DOCKER_IMAGE, DEEP_VARIANT_DOCKER_IMAGE + ":" + DEEP_VARIANT_VERSION);
        args.put(DeepVariantArguments.MODEL, DEEP_VARIANT_MODEL);
        args.put(DeepVariantArguments.STAGING, outDirGcsUri + DEEP_VARIANT_STAGING_DIR_NAME);
        args.put(DeepVariantArguments.OUTFILE, outfileGcsUri);
        args.put(DeepVariantArguments.BAM, bamUri);
        args.put(DeepVariantArguments.BAI, baiUri);
        args.put(DeepVariantArguments.REF, ref);
        args.put(DeepVariantArguments.REF_BAI, refIndex);
        args.put(DeepVariantArguments.SAMPLE_NAME, sampleName);
        args.put(DeepVariantArguments.JOB_NAME_PREFIX, jobNamePrefix);
        args.put(DeepVariantArguments.OPERATION_LABEL, jobNamePrefix);

        deepVariantOptions.getMakeExamplesCoresPerWorker().ifPresent(value -> args.put(DeepVariantArguments.MAKE_EXAMPLES_CORES_PER_WORKER, String.valueOf(value)));
        deepVariantOptions.getMakeExamplesRamPerWorker().ifPresent(value -> args.put(DeepVariantArguments.MAKE_EXAMPLES_RAM_PER_WORKER, String.valueOf(value)));
        deepVariantOptions.getMakeExamplesDiskPerWorker().ifPresent(value -> args.put(DeepVariantArguments.MAKE_EXAMPLES_DISK_PER_WORKER, String.valueOf(value)));

        deepVariantOptions.getCallVariantsCoresPerWorker().ifPresent(value -> args.put(DeepVariantArguments.CALL_VARIANTS_CORES_PER_WORKER, String.valueOf(value)));
        deepVariantOptions.getCallVariantsRamPerWorker().ifPresent(value -> args.put(DeepVariantArguments.CALL_VARIANTS_RAM_PER_WORKER, String.valueOf(value)));
        deepVariantOptions.getCallVariantsDiskPerWorker().ifPresent(value -> args.put(DeepVariantArguments.CALL_VARIANTS_DISK_PER_WORKER, String.valueOf(value)));

        deepVariantOptions.getPostprocessVariantsCores().ifPresent(value -> args.put(DeepVariantArguments.POSTPROCESS_VARINATS_CORES, String.valueOf(value)));
        deepVariantOptions.getPostprocessVariantsRam().ifPresent(value -> args.put(DeepVariantArguments.POSTPROCESS_VARINATS_RAM, String.valueOf(value)));
        deepVariantOptions.getPostprocessVariantsDisk().ifPresent(value -> args.put(DeepVariantArguments.POSTPROCESS_VARINATS_DISK, String.valueOf(value)));

        deepVariantOptions.getMakeExamplesWorkers().ifPresent(value -> args.put(DeepVariantArguments.MAKE_EXAMPLES_WORKERS, String.valueOf(value)));
        deepVariantOptions.getCallVariantsWorkers().ifPresent(value -> args.put(DeepVariantArguments.CALL_VARIANTS_WORKERS, String.valueOf(value)));
        deepVariantOptions.getPreemptible().filter(value -> value).ifPresent(value -> args.put(DeepVariantArguments.PREEMPTIBLE, ""));
        deepVariantOptions.getMaxPremptibleTries().ifPresent(value -> args.put(DeepVariantArguments.MAX_PREEMPTIBLE_TRIES, String.valueOf(value)));
        deepVariantOptions.getMaxNonPremptibleTries().ifPresent(value -> args.put(DeepVariantArguments.MAX_NON_PREEMPTIBLE_TRIES, String.valueOf(value)));
        deepVariantOptions.getDeepVariantShards().ifPresent(value -> args.put(DeepVariantArguments.SHARDS, String.valueOf(value)));

        List<String> command = new ArrayList<>();
        command.add(DEEP_VARIANT_RUNNER_PATH);
        for (Map.Entry<DeepVariantArguments, String> entry : args.entrySet()) {
            command.add(entry.getKey().getArgForCommand());
            command.add(entry.getValue());
        }
        LOG.info(String.format("Deep Variant command generated: \n%s\n", String.join("\n", command)));
        return command;
    }

}
