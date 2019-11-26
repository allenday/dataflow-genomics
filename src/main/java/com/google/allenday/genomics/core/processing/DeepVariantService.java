package com.google.allenday.genomics.core.processing;

import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.processing.lifesciences.LifeSciencesService;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import com.google.allenday.genomics.core.utils.ResourceProvider;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Service that provides access to Google Cloud Lifesciences API due to exevute Deep Variant processing.
 * <p>
 * Example of CLI command:
 * "--project cannabis-3k "+
 * "--region us-west1-* "+
 * "--docker_image gcr.io/deepvariant-docker/deepvariant:0.8.0 "+
 * "--outfile gs://cannabis-3k-deep-variant/output.vcf "+
 * "--staging gs://cannabis-3k-deep-variant/deep-variant-staging "+
 * "--model gs://deepvariant/models/DeepVariant/0.8.0/DeepVariant-inception_v3-0.8.0+data-wgs_standard "+
 * "--bam gs://cannabis-3k-results/cannabis_processing_output__dv_stage/2019-11-05--14-49-55-UTC/result_merged_bam/SRS1757973_AGQN03.merged.sorted.bam "+
 * "--bai gs://cannabis-3k-results/cannabis_processing_output__dv_stage/2019-11-05--14-49-55-UTC/result_merged_bam/SRS1757973_AGQN03.merged.sorted.bam.bai "+
 * "--ref gs://cannabis-3k/reference/AGQN03/AGQN03.fa "+
 * "--ref_fai gs://cannabis-3k/reference/AGQN03/AGQN03.fa.fai";
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

    private final static String DEFAULT_DEEP_VARIANT_LOCATION_REGION = "us-central1";
    private final static int DEFAULT_MAKE_EXAMPLES_CORES_PER_WORKER = 4;
    private final static int DEFAULT_MAKE_EXAMPLES_RAM_PER_WORKER = 20;
    private final static int DEFAULT_MAKE_EXAMPLES_DISK_PER_WORKER = 50;
    private final static int DEFAULT_CALL_VARIANTS_CORES_PER_WORKER = 4;
    private final static int DEFAULT_CALL_VARIANTS_RAM_PER_WORKER = 20;
    private final static int DEFAULT_CALL_VARIANTS_DISK_PER_WORKER = 50;
    private final static int DEFAULT_POSTPROCESS_VARINATS_CORES = 4;
    private final static int DEFAULT_POSTPROCESS_VARINATS_RAM = 20;
    private final static int DEFAULT_POSTPROCESS_VARINATS_DISK = 50;

    private String region;
    private Integer makeExamplesCoresPerWorker;
    private Integer makeExamplesRamPerWorker;
    private Integer makeExamplesDiskPerWorker;
    private Integer callVariantsCoresPerWorker;
    private Integer callVariantsRamPerWorker;
    private Integer callVariantsDiskPerWorker;
    private Integer postprocessVariantsCores;
    private Integer postprocessVariantsRam;
    private Integer postprocessVariantsDisk;

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

    public DeepVariantService(ReferencesProvider referencesProvider, LifeSciencesService lifeSciencesService) {
        this.referencesProvider = referencesProvider;
        this.lifeSciencesService = lifeSciencesService;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setMakeExamplesCoresPerWorker(Integer makeExamplesCoresPerWorker) {
        this.makeExamplesCoresPerWorker = makeExamplesCoresPerWorker;
    }

    public void setMakeExamplesRamPerWorker(Integer makeExamplesRamPerWorker) {
        this.makeExamplesRamPerWorker = makeExamplesRamPerWorker;
    }

    public void setMakeExamplesDiskPerWorker(Integer makeExamplesDiskPerWorker) {
        this.makeExamplesDiskPerWorker = makeExamplesDiskPerWorker;
    }

    public void setCallVariantsCoresPerWorker(Integer callVariantsCoresPerWorker) {
        this.callVariantsCoresPerWorker = callVariantsCoresPerWorker;
    }

    public void setCallVariantsRamPerWorker(Integer callVariantsRamPerWorker) {
        this.callVariantsRamPerWorker = callVariantsRamPerWorker;
    }

    public void setCallVariantsDiskPerWorker(Integer callVariantsDiskPerWorker) {
        this.callVariantsDiskPerWorker = callVariantsDiskPerWorker;
    }

    public void setPostprocessVariantsCores(Integer postprocessVariantsCores) {
        this.postprocessVariantsCores = postprocessVariantsCores;
    }

    public void setPostprocessVariantsRam(Integer postprocessVariantsRam) {
        this.postprocessVariantsRam = postprocessVariantsRam;
    }

    public void setPostprocessVariantsDisk(Integer postprocessVariantsDisk) {
        this.postprocessVariantsDisk = postprocessVariantsDisk;
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
        ;
        String selectedRegion = Optional.ofNullable(region).orElse(DEFAULT_DEEP_VARIANT_LOCATION_REGION);

        //TODO temp
        selectedRegion = "us-west2";
        Pair<Boolean, String> operationResult = lifeSciencesService.runLifesciencesPipelineWithLogging(actionCommands,
                DEEP_VARIANT_RUNNER_IMAGE_URI, outDirGcsUri, selectedRegion, DEEP_VARIANT_MACHINE_TYPE,
                resourceProvider.getProjectNumber(), outFilePrefix);

        return Triplet.with(outFileUri, operationResult.getValue0(), operationResult.getValue1());
    }


    private List<String> buildCommand(ResourceProvider resourceProvider,
                                      String outDirGcsUri, String outfileGcsUri, String bamUri, String baiUri,
                                      String ref, String refIndex,
                                      String sampleName, String jobNamePrefix) {
        Map<DeepVariantArguments, String> args = new HashMap<>();

        args.put(DeepVariantArguments.PROJECT, resourceProvider.getProjectId());
        args.put(DeepVariantArguments.ZONES, (region != null ? region : DEFAULT_DEEP_VARIANT_LOCATION_REGION) + "-*");
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

        int makeExampleWorkersCount = 1;
        int makeExampleCoresPerWorkerCount = makeExamplesCoresPerWorker != null
                ? makeExamplesCoresPerWorker : DEFAULT_MAKE_EXAMPLES_CORES_PER_WORKER;

        args.put(DeepVariantArguments.SHARDS, String.valueOf(makeExampleCoresPerWorkerCount*makeExampleWorkersCount));

        args.put(DeepVariantArguments.MAKE_EXAMPLES_CORES_PER_WORKER, String.valueOf(makeExampleCoresPerWorkerCount));
        args.put(DeepVariantArguments.MAKE_EXAMPLES_RAM_PER_WORKER, String.valueOf(makeExamplesRamPerWorker != null
                ? makeExamplesRamPerWorker : DEFAULT_MAKE_EXAMPLES_RAM_PER_WORKER));
        args.put(DeepVariantArguments.MAKE_EXAMPLES_DISK_PER_WORKER, String.valueOf(makeExamplesDiskPerWorker != null
                ? makeExamplesDiskPerWorker : DEFAULT_MAKE_EXAMPLES_DISK_PER_WORKER));

        args.put(DeepVariantArguments.CALL_VARIANTS_CORES_PER_WORKER, String.valueOf(callVariantsCoresPerWorker != null
                ? callVariantsCoresPerWorker : DEFAULT_CALL_VARIANTS_CORES_PER_WORKER));
        args.put(DeepVariantArguments.CALL_VARIANTS_RAM_PER_WORKER, String.valueOf(callVariantsRamPerWorker != null
                ? callVariantsRamPerWorker : DEFAULT_CALL_VARIANTS_RAM_PER_WORKER));
        args.put(DeepVariantArguments.CALL_VARIANTS_DISK_PER_WORKER, String.valueOf(callVariantsDiskPerWorker != null
                ? callVariantsDiskPerWorker : DEFAULT_CALL_VARIANTS_DISK_PER_WORKER));

        args.put(DeepVariantArguments.POSTPROCESS_VARINATS_CORES, String.valueOf(postprocessVariantsCores != null
                ? postprocessVariantsCores : DEFAULT_POSTPROCESS_VARINATS_CORES));
        args.put(DeepVariantArguments.POSTPROCESS_VARINATS_RAM, String.valueOf(postprocessVariantsRam != null
                ? postprocessVariantsRam : DEFAULT_POSTPROCESS_VARINATS_RAM));
        args.put(DeepVariantArguments.POSTPROCESS_VARINATS_DISK, String.valueOf(postprocessVariantsDisk != null
                ? postprocessVariantsDisk : DEFAULT_POSTPROCESS_VARINATS_DISK));

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
