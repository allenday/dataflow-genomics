package com.google.allenday.genomics.core.processing.lifesciences;

import com.google.allenday.genomics.core.processing.DeepVariantService;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.lifesciences.v2beta.CloudLifeSciences;
import com.google.api.services.lifesciences.v2beta.model.*;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LifeSciencesService implements Serializable {

    private Logger LOG = LoggerFactory.getLogger(DeepVariantService.class);
    private final static int STATUS_UPDATE_PERIOD = 10000;
    private final static String LIFE_SCIENCES_CREDENTIAL_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
    private final static String PARENT_PATH_PATTERN = "projects/%d/locations/%s";

    private CloudLifeSciences buildCloudLifeSciences() throws IOException {
        HttpTransport httpTransport = new HttpTransportOptions.DefaultHttpTransportFactory().create();
        JsonFactory jsonFactory = new JacksonFactory();

        GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault();
        if (googleCredentials.createScopedRequired()) {
            googleCredentials = googleCredentials.createScoped(Collections.singletonList(LIFE_SCIENCES_CREDENTIAL_SCOPE));

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


    public Pair<Boolean, String> runLifesciencesPipelineWithLogging(List<String> actionCommands, String actionImageUri, String logsDir,
                                                                    String region, String machineType, long projectNumber, String operationTag) {
        try {
            CloudLifeSciences cloudLifeSciences = buildCloudLifeSciences();
            Pipeline pipeline = new Pipeline()
                    .setActions(Arrays.asList(
                            new Action()
                                    .setCommands(actionCommands)
                                    .setImageUri(actionImageUri),
                            buildLoggingAction(logsDir)
                    ))
                    .setResources(new Resources()
                            .setRegions(Collections.singletonList(region))
                            .setVirtualMachine(new VirtualMachine()
                                    .setMachineType(machineType)));

            RunPipelineRequest runPipelineRequest = new RunPipelineRequest().setPipeline(pipeline);
            CloudLifeSciences.Projects.Locations.Pipelines.Run run = cloudLifeSciences
                    .projects().locations().pipelines().run(String.format(PARENT_PATH_PATTERN, projectNumber, region), runPipelineRequest);
            Operation runOperation = run.execute();

            long start = System.currentTimeMillis();
            LOG.info(String.format("LifeSciences pipeline started with metadata %s (%s)", runOperation.getMetadata().toString(), operationTag));

            String operationName = runOperation.getName();
            CloudLifeSciences.Projects.Locations.Operations.Get getStatusRequest = cloudLifeSciences
                    .projects().locations().operations().get(operationName);
            boolean isProcessing = true;

            boolean success = false;
            String msg = String.format("Not processed %s", operationTag);
            while (isProcessing) {
                Thread.sleep(STATUS_UPDATE_PERIOD);
                try {
                    Operation getOperation = getStatusRequest.execute();
                    isProcessing = getOperation.getDone() == null || !getOperation.getDone();

                    long durationInSec = (System.currentTimeMillis() - start) / 1000;
                    if (isProcessing) {
                        LOG.info(String.format("LifeSciences operation %s is still working (%d sec)(%s)",
                                operationName, durationInSec, operationTag));
                    } else {
                        success = getOperation.getError() == null;
                        msg = success
                                ? String.format("LifeSciences operation %s finished (%d sec)(%s)",
                                operationName, durationInSec, operationTag)
                                : String.format("LifeSciences operation %s failed with %s (%d sec)(%s)",
                                operationName, getOperation.getError().getCode() + " " + getOperation.getError().getMessage(), durationInSec, operationTag);

                        LOG.info(msg);
                    }
                } catch (GoogleJsonResponseException googleJsonException) {
                    LOG.error(googleJsonException.getMessage());
                }
            }
            return Pair.with(success, msg);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return Pair.with(false, e.getMessage());
        }
    }
}
