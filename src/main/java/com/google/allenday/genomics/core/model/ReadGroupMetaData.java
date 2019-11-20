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
public class ReadGroupMetaData implements Serializable {

    protected int avgSpotLen;
    @Nullable
    protected String bioSample;
    @Nullable
    protected String datastoreProvider;
    @Nullable
    protected String datastoreRegion;
    @Nullable
    protected String experiment;
    @Nullable
    protected int insertSize;
    @Nullable
    protected String libraryName;
    protected int numBases;
    protected int numBytes;

    @Nullable
    protected String sraSample;
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


    public ReadGroupMetaData() {
    }

    public ReadGroupMetaData(String sraSample) {
        this.sraSample = sraSample;
    }

    public int getAvgSpotLen() {
        return avgSpotLen;
    }

    public String getBioSample() {
        return bioSample;
    }

    public String getDatastoreProvider() {
        return datastoreProvider;
    }

    public String getDatastoreRegion() {
        return datastoreRegion;
    }

    public String getExperiment() {
        return experiment;
    }

    public int getInsertSize() {
        return insertSize;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public int getNumBases() {
        return numBases;
    }

    public int getNumBytes() {
        return numBytes;
    }

    public String getSraSample() {
        return sraSample;
    }

    public String getAssayType() {
        return assayType;
    }

    public String getBioProject() {
        return bioProject;
    }

    public String getCenterName() {
        return centerName;
    }

    public String getConsent() {
        return consent;
    }

    public String getDatastoreFiletype() {
        return datastoreFiletype;
    }

    public String getInstrument() {
        return instrument;
    }

    public String getLibrarySelection() {
        return librarySelection;
    }

    public String getLibrarySource() {
        return librarySource;
    }

    public String getLoadDate() {
        return loadDate;
    }

    public String getOrganism() {
        return organism;
    }

    public String getPlatform() {
        return platform;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public String getSraStudy() {
        return sraStudy;
    }


   /* public <T> TsetAvgSpotLen(int avgSpotLen) {
        this.avgSpotLen = avgSpotLen;
        return (T) this;
    }

    public ReadGroupMetaData setBioSample(String bioSample) {
        this.bioSample = bioSample;
        return this;
    }

    public ReadGroupMetaData setDatastoreProvider(String datastoreProvider) {
        this.datastoreProvider = datastoreProvider;
        return this;
    }

    public ReadGroupMetaData setDatastoreRegion(String datastoreRegion) {
        this.datastoreRegion = datastoreRegion;
        return this;
    }

    public ReadGroupMetaData setExperiment(String experiment) {
        this.experiment = experiment;
        return this;
    }

    public ReadGroupMetaData setInsertSize(int insertSize) {
        this.insertSize = insertSize;
        return this;
    }

    public ReadGroupMetaData setLibraryName(String libraryName) {
        this.libraryName = libraryName;
        return this;
    }

    public ReadGroupMetaData setNumBases(int numBases) {
        this.numBases = numBases;
        return this;
    }

    public ReadGroupMetaData setNumBytes(int numBytes) {
        this.numBytes = numBytes;
        return this;
    }

    public ReadGroupMetaData setAssayType(String assayType) {
        this.assayType = assayType;
        return this;
    }

    public ReadGroupMetaData setBioProject(String bioProject) {
        this.bioProject = bioProject;
        return this;
    }

    public ReadGroupMetaData setCenterName(String centerName) {
        this.centerName = centerName;
        return this;
    }

    public ReadGroupMetaData setConsent(String consent) {
        this.consent = consent;
        return this;
    }

    public ReadGroupMetaData setDatastoreFiletype(String datastoreFiletype) {
        this.datastoreFiletype = datastoreFiletype;
        return this;
    }

    public ReadGroupMetaData setInstrument(String instrument) {
        this.instrument = instrument;
        return this;
    }

    public ReadGroupMetaData setLibrarySelection(String librarySelection) {
        this.librarySelection = librarySelection;
        return this;
    }

    public ReadGroupMetaData setLibrarySource(String librarySource) {
        this.librarySource = librarySource;
        return this;
    }

    public ReadGroupMetaData setLoadDate(String loadDate) {
        this.loadDate = loadDate;
        return this;
    }

    public ReadGroupMetaData setOrganism(String organism) {
        this.organism = organism;
        return this;
    }

    public ReadGroupMetaData setPlatform(String platform) {
        this.platform = platform;
        return this;
    }

    public ReadGroupMetaData setReleaseDate(String releaseDate) {
        this.releaseDate = releaseDate;
        return this;
    }

    public ReadGroupMetaData setSraStudy(String sraStudy) {
        this.sraStudy = sraStudy;
        return this;
    }*/

    public void setAvgSpotLen(int avgSpotLen) {
        this.avgSpotLen = avgSpotLen;
    }

    public void setBioSample(String bioSample) {
        this.bioSample = bioSample;
    }

    public void setDatastoreProvider(String datastoreProvider) {
        this.datastoreProvider = datastoreProvider;
    }

    public void setDatastoreRegion(String datastoreRegion) {
        this.datastoreRegion = datastoreRegion;
    }

    public void setExperiment(String experiment) {
        this.experiment = experiment;
    }

    public void setInsertSize(int insertSize) {
        this.insertSize = insertSize;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    public void setNumBases(int numBases) {
        this.numBases = numBases;
    }

    public void setNumBytes(int numBytes) {
        this.numBytes = numBytes;
    }

    public void setSraSample(String sraSample) {
        this.sraSample = sraSample;
    }

    public void setAssayType(String assayType) {
        this.assayType = assayType;
    }

    public void setBioProject(String bioProject) {
        this.bioProject = bioProject;
    }

    public void setCenterName(String centerName) {
        this.centerName = centerName;
    }

    public void setConsent(String consent) {
        this.consent = consent;
    }

    public void setDatastoreFiletype(String datastoreFiletype) {
        this.datastoreFiletype = datastoreFiletype;
    }

    public void setInstrument(String instrument) {
        this.instrument = instrument;
    }

    public void setLibrarySelection(String librarySelection) {
        this.librarySelection = librarySelection;
    }

    public void setLibrarySource(String librarySource) {
        this.librarySource = librarySource;
    }

    public void setLoadDate(String loadDate) {
        this.loadDate = loadDate;
    }

    public void setOrganism(String organism) {
        this.organism = organism;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public void setReleaseDate(String releaseDate) {
        this.releaseDate = releaseDate;
    }

    public void setSraStudy(String sraStudy) {
        this.sraStudy = sraStudy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReadGroupMetaData that = (ReadGroupMetaData) o;
        return avgSpotLen == that.avgSpotLen &&
                insertSize == that.insertSize &&
                numBases == that.numBases &&
                numBytes == that.numBytes &&
                Objects.equals(bioSample, that.bioSample) &&
                Objects.equals(datastoreProvider, that.datastoreProvider) &&
                Objects.equals(datastoreRegion, that.datastoreRegion) &&
                Objects.equals(experiment, that.experiment) &&
                Objects.equals(libraryName, that.libraryName) &&
                Objects.equals(sraSample, that.sraSample) &&
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
        return Objects.hash(avgSpotLen, bioSample, datastoreProvider, datastoreRegion, experiment, insertSize, libraryName, numBases, numBytes, sraSample, assayType, bioProject, centerName, consent, datastoreFiletype, instrument, librarySelection, librarySource, loadDate, organism, platform, releaseDate, sraStudy);
    }

    @Override
    public String toString() {
        return "ReadGroupMetaData{" +
                "avgSpotLen=" + avgSpotLen +
                ", bioSample='" + bioSample + '\'' +
                ", datastoreProvider='" + datastoreProvider + '\'' +
                ", datastoreRegion='" + datastoreRegion + '\'' +
                ", experiment='" + experiment + '\'' +
                ", insertSize=" + insertSize +
                ", libraryName='" + libraryName + '\'' +
                ", numBases=" + numBases +
                ", numBytes=" + numBytes +
                ", sraSample='" + sraSample + '\'' +
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

