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
    protected String experiment;
    @Nullable
    protected String bioSample;
    @Nullable
    protected String assayType;
    @Nullable
    protected String bioProject;
    @Nullable
    protected String centerName;
    @Nullable
    protected String consent;
    @Nullable
    protected String datastoreFiletype;
    @Nullable
    protected String instrument;
    @Nullable
    protected String librarySelection;
    @Nullable
    protected String librarySource;
    @Nullable
    protected String loadDate;
    @Nullable
    protected String organism;
    @Nullable
    protected String platform;
    @Nullable
    protected String releaseDate;
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

    public String getExperiment() {
        return experiment;
    }

    public void setExperiment(String experiment) {
        this.experiment = experiment;
    }

    public String getBioSample() {
        return bioSample;
    }

    public void setBioSample(String bioSample) {
        this.bioSample = bioSample;
    }

    public String getAssayType() {
        return assayType;
    }

    public void setAssayType(String assayType) {
        this.assayType = assayType;
    }

    public String getBioProject() {
        return bioProject;
    }

    public void setBioProject(String bioProject) {
        this.bioProject = bioProject;
    }

    public String getCenterName() {
        return centerName;
    }

    public void setCenterName(String centerName) {
        this.centerName = centerName;
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

    public String getInstrument() {
        return instrument;
    }

    public void setInstrument(String instrument) {
        this.instrument = instrument;
    }

    public String getLibrarySelection() {
        return librarySelection;
    }

    public void setLibrarySelection(String librarySelection) {
        this.librarySelection = librarySelection;
    }

    public String getLibrarySource() {
        return librarySource;
    }

    public void setLibrarySource(String librarySource) {
        this.librarySource = librarySource;
    }

    public String getLoadDate() {
        return loadDate;
    }

    public void setLoadDate(String loadDate) {
        this.loadDate = loadDate;
    }

    public String getOrganism() {
        return organism;
    }

    public void setOrganism(String organism) {
        this.organism = organism;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(String releaseDate) {
        this.releaseDate = releaseDate;
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
                Objects.equals(experiment, that.experiment) &&
                Objects.equals(bioSample, that.bioSample) &&
                Objects.equals(assayType, that.assayType) &&
                Objects.equals(bioProject, that.bioProject) &&
                Objects.equals(centerName, that.centerName) &&
                Objects.equals(consent, that.consent) &&
                Objects.equals(datastoreFiletype, that.datastoreFiletype) &&
                Objects.equals(instrument, that.instrument) &&
                Objects.equals(librarySelection, that.librarySelection) &&
                Objects.equals(librarySource, that.librarySource) &&
                Objects.equals(loadDate, that.loadDate) &&
                Objects.equals(organism, that.organism) &&
                Objects.equals(platform, that.platform) &&
                Objects.equals(releaseDate, that.releaseDate) &&
                Objects.equals(sraStudy, that.sraStudy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sraSample, experiment, bioSample, assayType, bioProject, centerName, consent, datastoreFiletype, instrument, librarySelection, librarySource, loadDate, organism, platform, releaseDate, sraStudy);
    }

    @Override
    public String toString() {
        return "GeneReadGroupMetaData{" +
                "sraSample='" + sraSample + '\'' +
                ", experiment='" + experiment + '\'' +
                ", bioSample='" + bioSample + '\'' +
                ", assayType='" + assayType + '\'' +
                ", bioProject='" + bioProject + '\'' +
                ", centerName='" + centerName + '\'' +
                ", consent='" + consent + '\'' +
                ", datastoreFiletype='" + datastoreFiletype + '\'' +
                ", instrument='" + instrument + '\'' +
                ", librarySelection='" + librarySelection + '\'' +
                ", librarySource='" + librarySource + '\'' +
                ", loadDate='" + loadDate + '\'' +
                ", organism='" + organism + '\'' +
                ", platform='" + platform + '\'' +
                ", releaseDate='" + releaseDate + '\'' +
                ", sraStudy='" + sraStudy + '\'' +
                '}';
    }
}

