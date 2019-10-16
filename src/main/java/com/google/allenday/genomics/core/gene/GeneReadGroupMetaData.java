package com.google.allenday.genomics.core.gene;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Objects;

//TODO

/**
 *
 */
@DefaultCoder(AvroCoder.class)
public class GeneReadGroupMetaData implements Serializable {

    protected String projectName;
    protected String projectId;
    protected String bioSample;
    protected String sraSample;

    public GeneReadGroupMetaData() {
    }

    public GeneReadGroupMetaData(String projectName, String projectId, String bioSample, String sraSample) {
        this.projectName = projectName;
        this.projectId = projectId;
        this.bioSample = bioSample;
        this.sraSample = sraSample;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getBioSample() {
        return bioSample;
    }

    public String getSraSample() {
        return sraSample;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeneReadGroupMetaData that = (GeneReadGroupMetaData) o;
        return Objects.equals(projectName, that.projectName) &&
                Objects.equals(projectId, that.projectId) &&
                Objects.equals(bioSample, that.bioSample) &&
                Objects.equals(sraSample, that.sraSample);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectName, projectId, bioSample, sraSample);
    }

    @Override
    public String toString() {
        return "GeneExampleMetaData{" +
                "projectName='" + projectName + '\'' +
                ", projectId='" + projectId + '\'' +
                ", bioSample='" + bioSample + '\'' +
                ", sraSample='" + sraSample + '\'' +
                '}';
    }
}

