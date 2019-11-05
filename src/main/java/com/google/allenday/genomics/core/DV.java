package com.google.allenday.genomics.core;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.lifesciences.v2beta.CloudLifeSciences;
import com.google.api.services.lifesciences.v2beta.model.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class DV {

    public static void main(String[] args) {
        HttpTransport httpTransport = new ApacheHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();

        try {
            GoogleCredential credential =
                    GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);
            if (credential.createScopedRequired()) {
                credential = credential.createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }

            Pipeline pipeline = new Pipeline()
                    .setActions(
                            Arrays.asList(
                                    new Action()
                                            .setCommands(Arrays.asList("-c", "ls -la"))
                                            .setEntrypoint("bash")
                                            .setImageUri("google/cloud-sdk"),
                                    new Action()
                                            .setCommands(Arrays.asList("/bin/sh", "-c", "gsutil -m -q cp /google/logs/output gs://cannabis-3k-deep-variant/my-path/example.log"))
                                            .setImageUri("google/cloud-sdk")
                                            .setAlwaysRun(true)
                                            .setEnvironment(Collections.singletonMap("logging", "gs://cannabis-3k-deep-variant/my-path-java/example.log"))
                            )
                    )
                    .setResources(new Resources()
                            .setRegions(Collections.singletonList("us-central1"))
                            .setVirtualMachine(new VirtualMachine()
                                    .setMachineType("n1-standard-4")));

            RunPipelineRequest runPipelineRequest = new RunPipelineRequest().setPipeline(pipeline);

            HttpRequestInitializer initializer = credential;

            CloudLifeSciences.Projects.Locations.Pipelines.Run run = null;
            run = new CloudLifeSciences(httpTransport, jsonFactory, initializer)
                    .projects().locations().pipelines().run(
                            "projects/982060272235/locations/us-central1", runPipelineRequest);
            Operation operation = run.execute();
            System.out.println(operation.getMetadata());
            System.out.println(operation.getResponse());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
