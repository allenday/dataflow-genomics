package com.google.allenday.genomics.core.model;

import org.apache.avro.reflect.Nullable;
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

    @Nullable
    protected String sraSample;
    @Nullable
    protected String bioSample;
    @Nullable
    protected String bioProject;
    @Nullable
    protected String consent;
    @Nullable
    protected String datastoreFiletype;
    @Nullable
    protected String organism;
    @Nullable
    protected String sraStudy;


    public GeneReadGroupMetaData() {
    }

    public GeneReadGroupMetaData(String sraSample) {
        this.sraSample = sraSample;
    }

    public String getSraSample() {
        return sraSample;
    }

    public void setSraSample(String sraSample) {
        this.sraSample = sraSample;
    }


    public String getBioSample() {
        return bioSample;
    }

    public void setBioSample(String bioSample) {
        this.bioSample = bioSample;
    }

    public String getBioProject() {
        return bioProject;
    }

    public void setBioProject(String bioProject) {
        this.bioProject = bioProject;
    }


    public String getConsent() {
        return consent;
    }

    public void setConsent(String consent) {
        this.consent = consent;
    }

    public String getDatastoreFiletype() {
        return datastoreFiletype;
    }

    public void setDatastoreFiletype(String datastoreFiletype) {
        this.datastoreFiletype = datastoreFiletype;
    }

    public String getOrganism() {
        return organism;
    }

    public void setOrganism(String organism) {
        this.organism = organism;
    }

    public String getSraStudy() {
        return sraStudy;
    }

    public void setSraStudy(String sraStudy) {
        this.sraStudy = sraStudy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeneReadGroupMetaData that = (GeneReadGroupMetaData) o;
        return Objects.equals(sraSample, that.sraSample) &&
                Objects.equals(bioSample, that.bioSample) &&
                Objects.equals(bioProject, that.bioProject) &&
                Objects.equals(consent, that.consent) &&
                Objects.equals(datastoreFiletype, that.datastoreFiletype) &&
                Objects.equals(organism, that.organism) &&
                Objects.equals(sraStudy, that.sraStudy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sraSample, bioSample, bioProject, consent, datastoreFiletype, organism, sraStudy);
    }

    @Override
    public String toString() {
        return "GeneReadGroupMetaData{" +
                "sraSample='" + sraSample + '\'' +
                ", bioSample='" + bioSample + '\'' +
                ", bioProject='" + bioProject + '\'' +
                ", consent='" + consent + '\'' +
                ", datastoreFiletype='" + datastoreFiletype + '\'' +
                ", organism='" + organism + '\'' +
                ", sraStudy='" + sraStudy + '\'' +
                '}';
    }
}

