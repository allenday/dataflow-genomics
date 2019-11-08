package com.google.allenday.genomics.core.processing;

import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import com.google.allenday.genomics.core.utils.ResourceProvider;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.lifesciences.v2beta.CloudLifeSciences;
import com.google.api.services.lifesciences.v2beta.model.*;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Service that provides access to Google Cloud Lifesciences API due to exevute Deep Variant processing.
 * <p>
 * Example of CLI command:
 * "--project cannabis-3k "+
 * "--zones us-west1-* "+
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
    private final static String DEEP_VARIANT_VERSION = "0.8.0";
    private final static String DEEP_VARIANT_MODEL = "gs://deepvariant/models/DeepVariant/0.8.0/DeepVariant-inception_v3-0.8.0+data-wgs_standard";

    private final static String DEEP_VARIANT_LOCATION_ZONE = "us-central1";
    private final static String DEEP_VARIANT_ZONES = DEEP_VARIANT_LOCATION_ZONE + "-*";
    private final static String DEEP_VARIANT_STAGING_DIR_NAME = "deep-variant-staging";

    private final static String DEEP_VARIANT_MACHINE_TYPE = "n1-standard-16";
    private final static String DEEP_VARIANT_CREDENTIAL_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

    private final static String DEEP_VARIANT_PARENT_PATH_PATTERN = "projects/%d/locations/%s";

    private final static String DEEP_VARIANT_RESULT_EXTENSION = ".vcf";

    private final static int DEEP_VARIANT_STATUS_UPDATE_PERIOD = 10000;

    public static enum DeepVariantArguments {
        PROJECT("project"),
        ZONES("zones"),
        DOCKER_IMAGE("docker_image"),
        MODEL("model"),
        STAGING("staging"),
        OUTFILE("outfile"),
        BAM("bam"),
        BAI("bai"),
        REF("ref"),
        REF_BAI("ref_fai");

        private final String argName;

        DeepVariantArguments(String argName) {
            this.argName = argName;
        }

        public String getArgForCommand() {
            return "--" + argName;
        }
    }

    private ReferencesProvider referencesProvider;

    public DeepVariantService(ReferencesProvider referencesProvider) {
        this.referencesProvider = referencesProvider;
    }

    private CloudLifeSciences buildCloudLifeSciences() throws IOException {
        HttpTransport httpTransport = new HttpTransportOptions.DefaultHttpTransportFactory().create();
        JsonFactory jsonFactory = new JacksonFactory();

        GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault();
        if (googleCredentials.createScopedRequired()) {
            googleCredentials = googleCredentials.createScoped(Collections.singletonList(DEEP_VARIANT_CREDENTIAL_SCOPE));

        }
        HttpCredentialsAdapter httpCredentialsAdapter = new HttpCredentialsAdapter(googleCredentials);
        return new CloudLifeSciences(httpTransport, jsonFactory, httpCredentialsAdapter);
    }

    private Action buildLoggingAction(String logsDir) {
        String logsPath = logsDir + "runner_logs.log";
        return new Action()
                .setCommands(Arrays.asList("/bin/sh", "-c", String.format("gsutil -m -q cp /google/logs/output %s", logsPath)))
                .setImageUri("google/cloud-sdk")
                .setAlwaysRun(true);
    }

    public Triplet<String, Boolean, String> processExampleWithDeepVariant(ResourceProvider resourceProvider,
                                                                          String outDirGcsUri, String outFilePrefix,
                                                                          String bamUri, String baiUri,
                                                                          ReferenceDatabase referenceDatabase) {
        Pair<String, String> refUriWithIndex = referenceDatabase.getRefUriWithIndex(referencesProvider.getReferenceFileExtension());

        String outFileUri = outDirGcsUri + outFilePrefix + DEEP_VARIANT_RESULT_EXTENSION;
        CloudLifeSciences cloudLifeSciences = null;
        try {
            cloudLifeSciences = buildCloudLifeSciences();
            Pipeline pipeline = new Pipeline()
                    .setActions(Arrays.asList(
                            new Action()
                                    .setCommands(buildCommand(resourceProvider, outDirGcsUri, outFileUri, bamUri, baiUri,
                                            refUriWithIndex.getValue0(), refUriWithIndex.getValue1()))
                                    .setImageUri(DEEP_VARIANT_RUNNER_IMAGE_URI),
                            buildLoggingAction(outDirGcsUri)
                    ))
                    .setResources(new Resources()
                            .setRegions(Collections.singletonList(DEEP_VARIANT_LOCATION_ZONE))
                            .setVirtualMachine(new VirtualMachine()
                                    .setMachineType(DEEP_VARIANT_MACHINE_TYPE)));

            RunPipelineRequest runPipelineRequest = new RunPipelineRequest().setPipeline(pipeline);
            CloudLifeSciences.Projects.Locations.Pipelines.Run run = cloudLifeSciences
                    .projects().locations().pipelines().run(
                            String.format(DEEP_VARIANT_PARENT_PATH_PATTERN, resourceProvider.getProjectNumber(), DEEP_VARIANT_LOCATION_ZONE),
                            runPipelineRequest);
            Operation runOperation = run.execute();
            long start = System.currentTimeMillis();
            LOG.info(String.format("Deep Variant started with metadata %s (%s)", runOperation.getMetadata().toString(), outFilePrefix));

            String operationName = runOperation.getName();
            CloudLifeSciences.Projects.Locations.Operations.Get getStatusRequest = cloudLifeSciences
                    .projects().locations().operations().get(operationName);
            boolean isProcessing = true;

            boolean success = false;
            String msg = String.format("Not processed %s", outFilePrefix);
            while (isProcessing) {
                Thread.sleep(DEEP_VARIANT_STATUS_UPDATE_PERIOD);
                Operation getOperation = getStatusRequest.execute();
                isProcessing = getOperation.getDone() == null || !getOperation.getDone();

                if (isProcessing) {
                    LOG.info(String.format("Deep Variant operation %s is still working (%d sec)",
                            operationName, (System.currentTimeMillis() - start) / 1000));
                } else {
                    success = getOperation.getError() == null;
                    msg = success
                            ? String.format("Deep Variant operation %s finished (%d sec)", operationName, (System.currentTimeMillis() - start) / 1000)
                            : String.format("Deep Variant operation %s failed with %s (%d sec)", operationName, getOperation.getError().getCode() + " " + getOperation.getError().getMessage(), (System.currentTimeMillis() - start) / 1000);

                    LOG.info(msg);
                }
            }
            return Triplet.with(outFileUri, success, msg);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return Triplet.with(outFileUri, false, e.getMessage());
        }
    }


    private List<String> buildCommand(ResourceProvider resourceProvider,
                                      String outDirGcsUri, String outfileGcsUri, String bamUri, String baiUri,
                                      String ref, String refIndex) {
        Map<DeepVariantArguments, String> args = new HashMap<>();

        args.put(DeepVariantArguments.PROJECT, resourceProvider.getProjectId());
        args.put(DeepVariantArguments.ZONES, DEEP_VARIANT_ZONES);
        args.put(DeepVariantArguments.DOCKER_IMAGE, DEEP_VARIANT_DOCKER_IMAGE + ":" + DEEP_VARIANT_VERSION);
        args.put(DeepVariantArguments.MODEL, DEEP_VARIANT_MODEL);
        args.put(DeepVariantArguments.STAGING, outDirGcsUri + DEEP_VARIANT_STAGING_DIR_NAME);
        args.put(DeepVariantArguments.OUTFILE, outfileGcsUri);
        args.put(DeepVariantArguments.BAM, bamUri);
        args.put(DeepVariantArguments.BAI, baiUri);
        args.put(DeepVariantArguments.REF, ref);
        args.put(DeepVariantArguments.REF_BAI, refIndex);

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
