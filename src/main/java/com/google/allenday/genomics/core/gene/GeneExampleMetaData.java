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
public class GeneExampleMetaData extends GeneReadGroupMetaData implements Serializable {

    private String run;
    private String srcRawMetaData;
    private String comment = "";

    public GeneExampleMetaData() {
    }

    public GeneExampleMetaData(String project, String projectId, String bioSample, String sraSample, String run, String srcRawMetaData) {
        super(project, projectId, bioSample, sraSample);
        this.run = run;
        this.srcRawMetaData = srcRawMetaData;
    }

    public String getRun() {
        return run;
    }

    public String getSrcRawMetaData() {
        return srcRawMetaData;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GeneExampleMetaData that = (GeneExampleMetaData) o;
        return Objects.equals(run, that.run) &&
                Objects.equals(srcRawMetaData, that.srcRawMetaData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), run, srcRawMetaData);
    }

    @Override
    public String toString() {
        return "GeneExampleMetaData{" +
                "run='" + run + '\'' +
                ", srcRawMetaData='" + srcRawMetaData + '\'' +
                ", project='" + project + '\'' +
                ", projectId='" + projectId + '\'' +
                ", bioSample='" + bioSample + '\'' +
                ", sraSample='" + sraSample + '\'' +
                '}';
    }
}

