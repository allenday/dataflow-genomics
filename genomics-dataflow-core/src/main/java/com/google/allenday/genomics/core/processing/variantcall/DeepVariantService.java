package com.google.allenday.genomics.core.processing.variantcall;

import com.google.allenday.genomics.core.pipeline.DeepVariantOptions;
import com.google.allenday.genomics.core.gcp.LifeSciencesService;
import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import com.google.allenday.genomics.core.gcp.ResourceProvider;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service that provides access to Google Cloud Lifesciences API used to execute Deep Variant processing.
 */

public class DeepVariantService extends VariantCallingService {
    private Logger LOG = LoggerFactory.getLogger(DeepVariantService.class);

    private final static int MAX_JOB_NAME_PREFIX_LEN = 20;
    private final static String DEEP_VARIANT_RUNNER_IMAGE_URI = "gcr.io/cloud-genomics-pipelines/gcp-deepvariant-runner";

    private final static String DEEP_VARIANT_RUNNER_PATH = "/opt/deepvariant_runner/bin/gcp_deepvariant_runner";
    private final static String DEEP_VARIANT_DOCKER_IMAGE = "gcr.io/deepvariant-docker/deepvariant";
    private final static String DEEP_VARIANT_VERSION = "0.9.0";
    private final static String DEEP_VARIANT_MODEL = "gs://deepvariant/models/DeepVariant/0.9.0/DeepVariant-inception_v3-0.9.0+data-wgs_standard";

    private final static String DEEP_VARIANT_STAGING_DIR_NAME = "deep-variant-staging";

    private final static String DEEP_VARIANT_MACHINE_TYPE = "n1-standard-1";

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
        SHARDS("shards"),
        REGIONS("regions");
        private final String argName;

        DeepVariantArguments(String argName) {
            this.argName = argName;
        }

        public String getArgForCommand() {
            return "--" + argName;
        }
    }

    private LifeSciencesService lifeSciencesService;
    private DeepVariantOptions deepVariantOptions;

    public DeepVariantService(LifeSciencesService lifeSciencesService, DeepVariantOptions deepVariantOptions) {
        this.lifeSciencesService = lifeSciencesService;
        this.deepVariantOptions = deepVariantOptions;
    }

    @Override
    public void setup() {

    }

    @Override
    public Triplet<String, Boolean, String> processSampleWithVariantCaller(ResourceProvider resourceProvider,
                                                                           String outDirGcsUri,
                                                                           String outFileNameWithoutExt,
                                                                           String bamUri,
                                                                           String baiUri,
                                                                           String regions,
                                                                           ReferenceDatabase referenceDatabase,
                                                                           String sampleName) {
        String outFileUri = outDirGcsUri + outFileNameWithoutExt + DEEP_VARIANT_RESULT_EXTENSION;

        String jobNamePrefix = generateJobNamePrefix(outFileNameWithoutExt);
        List<String> actionCommands = buildCommand(resourceProvider, outDirGcsUri, outFileUri, bamUri, baiUri,
                referenceDatabase.getFastaGcsUri(), referenceDatabase.getFaiGcsUri(), regions, sampleName, jobNamePrefix);

        Pair<Boolean, String> operationResult = lifeSciencesService.runLifesciencesPipelineWithLogging(actionCommands,
                DEEP_VARIANT_RUNNER_IMAGE_URI, outDirGcsUri, deepVariantOptions.getControlPipelineWorkerRegion(),
                DEEP_VARIANT_MACHINE_TYPE, resourceProvider.getProjectNumber(), outFileNameWithoutExt);

        return Triplet.with(outFileUri, operationResult.getValue0(), operationResult.getValue1());
    }

    private String generateJobNamePrefix(String outFilePrefix) {
        String jobNamePrefix = outFilePrefix.toLowerCase().replace("-", "_").replace(".", "_") + "_";
        if (jobNamePrefix.length() > MAX_JOB_NAME_PREFIX_LEN) {
            jobNamePrefix = jobNamePrefix.substring(0, MAX_JOB_NAME_PREFIX_LEN);
        }
        return jobNamePrefix;
    }

    private List<String> buildCommand(ResourceProvider resourceProvider,
                                      String outDirGcsUri,
                                      String outfileGcsUri,
                                      String bamUri,
                                      String baiUri,
                                      String ref,
                                      String refIndex,
                                      String regions,
                                      String sampleName,
                                      String jobNamePrefix) {
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
        if (regions != null) {
            args.put(DeepVariantArguments.REGIONS, regions);
        }

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
