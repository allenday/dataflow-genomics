package com.google.allenday.genomics.core.gcp;

import com.google.cloud.ServiceOptions;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.resourcemanager.ResourceManagerOptions;

public class ResourceProvider {

    private String projectId;
    private ResourceManager resourceManager;

    private ResourceProvider(String projectId, ResourceManager resourceManager) {
        this.projectId = projectId;
        this.resourceManager = resourceManager;
    }

    public static ResourceProvider initialize() {
        String defaultProjectId = ServiceOptions.getDefaultProjectId();
        return new ResourceProvider(defaultProjectId, ResourceManagerOptions.getDefaultInstance().getService());
    }

    public String getProjectId() {
        return resourceManager.get(projectId).getProjectId();
    }

    public long getProjectNumber() {
        return resourceManager.get(projectId).getProjectNumber();
    }

}
