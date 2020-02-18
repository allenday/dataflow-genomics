package com.google.allenday.genomics.core.model;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

//TODO

/**
 *
 */
@DefaultCoder(AvroCoder.class)
public class SampleMetaData implements Serializable {

    private final static String IS_PAIRED_FLAG = "PAIRED";

    @Nullable
    protected Integer avgSpotLen;

    @Nullable
    protected String datastoreProvider;
    @Nullable
    protected String datastoreRegion;
    @Nullable
    protected Integer insertSize;
    @Nullable
    protected String libraryName;
    @Nullable
    protected Integer numBases;
    @Nullable
    protected Integer numBytes;
    @Nullable
    protected String experiment;
    @Nullable
    protected String instrument;
    @Nullable
    protected String platform;
    @Nullable
    protected String loadDate;
    @Nullable
    protected String releaseDate;
    @Nullable
    protected String assayType;
    @Nullable
    protected String centerName;
    @Nullable
    protected String librarySelection;
    @Nullable
    protected String librarySource;
    @Nullable
    protected String datastoreFiletype;
    @Nullable
    protected SraSampleId sraSample;
    @Nullable
    protected String bioSample;
    @Nullable
    protected String bioProject;
    @Nullable
    protected String consent;
    @Nullable
    protected String organism;
    @Nullable
    protected String sraStudy;

    private String runId;
    private String libraryLayout;

    @Nullable
    private String sampleName;

    private String srcRawMetaData;
    @Nullable
    private String comment;

    public SampleMetaData() {
    }

    public SampleMetaData(String sraSample, String runId, String libraryLayout, String platform, String srcRawMetaData) {
        this.sraSample = SraSampleId.create(sraSample);
        this.runId = runId;
        this.libraryLayout = libraryLayout;
        this.srcRawMetaData = srcRawMetaData;
        this.platform = platform;
    }

    public static SampleMetaData fromCsvLine(Parser parser, String csvLine) throws Parser.CsvParseException {
        return parser.parse(csvLine);
    }

    public static SampleMetaData createUnique(String rawMetaData, String libraryLayout, String platform) {
        String uniqueName = UUID.randomUUID().toString();
        return new SampleMetaData(
                "sraSample_" + uniqueName,
                "runId_" + uniqueName,
                libraryLayout,
                platform,
                rawMetaData);
    }

    public Integer getAvgSpotLen() {
        return avgSpotLen;
    }

    public void setAvgSpotLen(Integer avgSpotLen) {
        this.avgSpotLen = avgSpotLen;
    }

    public String getDatastoreProvider() {
        return datastoreProvider;
    }

    public void setDatastoreProvider(String datastoreProvider) {
        this.datastoreProvider = datastoreProvider;
    }

    public String getDatastoreRegion() {
        return datastoreRegion;
    }

    public void setDatastoreRegion(String datastoreRegion) {
        this.datastoreRegion = datastoreRegion;
    }

    public Integer getInsertSize() {
        return insertSize;
    }

    public void setInsertSize(Integer insertSize) {
        this.insertSize = insertSize;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    public Integer getNumBases() {
        return numBases;
    }

    public void setNumBases(Integer numBases) {
        this.numBases = numBases;
    }

    public Integer getNumBytes() {
        return numBytes;
    }

    public void setNumBytes(Integer numBytes) {
        this.numBytes = numBytes;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getLibraryLayout() {
        return libraryLayout;
    }

    public void setLibraryLayout(String libraryLayout) {
        this.libraryLayout = libraryLayout;
    }

    public String getSampleName() {
        return sampleName;
    }

    public String getSrcRawMetaData() {
        return srcRawMetaData;
    }

    public void setSrcRawMetaData(String srcRawMetaData) {
        this.srcRawMetaData = srcRawMetaData;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public boolean isPaired() {
        return libraryLayout.equals(IS_PAIRED_FLAG);
    }

    public String getExperiment() {
        return experiment;
    }

    public void setExperiment(String experiment) {
        this.experiment = experiment;
    }

    public String getInstrument() {
        return instrument;
    }

    public void setInstrument(String instrument) {
        this.instrument = instrument;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getLoadDate() {
        return loadDate;
    }

    public void setLoadDate(String loadDate) {
        this.loadDate = loadDate;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(String releaseDate) {
        this.releaseDate = releaseDate;
    }

    public String getAssayType() {
        return assayType;
    }

    public void setAssayType(String assayType) {
        this.assayType = assayType;
    }

    public String getCenterName() {
        return centerName;
    }

    public void setCenterName(String centerName) {
        this.centerName = centerName;
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

    public String getDatastoreFiletype() {
        return datastoreFiletype;
    }

    public void setDatastoreFiletype(String datastoreFiletype) {
        this.datastoreFiletype = datastoreFiletype;
    }

    public SraSampleId getSraSample() {
        return sraSample;
    }

    public void setSraSample(SraSampleId sraSample) {
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

    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleMetaData that = (SampleMetaData) o;
        return Objects.equals(avgSpotLen, that.avgSpotLen) &&
                Objects.equals(datastoreProvider, that.datastoreProvider) &&
                Objects.equals(datastoreRegion, that.datastoreRegion) &&
                Objects.equals(insertSize, that.insertSize) &&
                Objects.equals(libraryName, that.libraryName) &&
                Objects.equals(numBases, that.numBases) &&
                Objects.equals(numBytes, that.numBytes) &&
                Objects.equals(experiment, that.experiment) &&
                Objects.equals(instrument, that.instrument) &&
                Objects.equals(platform, that.platform) &&
                Objects.equals(loadDate, that.loadDate) &&
                Objects.equals(releaseDate, that.releaseDate) &&
                Objects.equals(assayType, that.assayType) &&
                Objects.equals(centerName, that.centerName) &&
                Objects.equals(librarySelection, that.librarySelection) &&
                Objects.equals(librarySource, that.librarySource) &&
                Objects.equals(datastoreFiletype, that.datastoreFiletype) &&
                Objects.equals(sraSample, that.sraSample) &&
                Objects.equals(bioSample, that.bioSample) &&
                Objects.equals(bioProject, that.bioProject) &&
                Objects.equals(consent, that.consent) &&
                Objects.equals(organism, that.organism) &&
                Objects.equals(sraStudy, that.sraStudy) &&
                Objects.equals(runId, that.runId) &&
                Objects.equals(libraryLayout, that.libraryLayout) &&
                Objects.equals(sampleName, that.sampleName) &&
                Objects.equals(srcRawMetaData, that.srcRawMetaData) &&
                Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(avgSpotLen, datastoreProvider, datastoreRegion, insertSize, libraryName, numBases, numBytes, experiment, instrument, platform, loadDate, releaseDate, assayType, centerName, librarySelection, librarySource, datastoreFiletype, sraSample, bioSample, bioProject, consent, organism, sraStudy, runId, libraryLayout, sampleName, srcRawMetaData, comment);
    }

    @Override
    public String toString() {
        return "SampleMetaData{" +
                "avgSpotLen=" + avgSpotLen +
                ", datastoreProvider='" + datastoreProvider + '\'' +
                ", datastoreRegion='" + datastoreRegion + '\'' +
                ", insertSize=" + insertSize +
                ", libraryName='" + libraryName + '\'' +
                ", numBases=" + numBases +
                ", numBytes=" + numBytes +
                ", experiment='" + experiment + '\'' +
                ", instrument='" + instrument + '\'' +
                ", platform='" + platform + '\'' +
                ", loadDate='" + loadDate + '\'' +
                ", releaseDate='" + releaseDate + '\'' +
                ", assayType='" + assayType + '\'' +
                ", centerName='" + centerName + '\'' +
                ", librarySelection='" + librarySelection + '\'' +
                ", librarySource='" + librarySource + '\'' +
                ", datastoreFiletype='" + datastoreFiletype + '\'' +
                ", sraSample=" + sraSample +
                ", bioSample='" + bioSample + '\'' +
                ", bioProject='" + bioProject + '\'' +
                ", consent='" + consent + '\'' +
                ", organism='" + organism + '\'' +
                ", sraStudy='" + sraStudy + '\'' +
                ", runId='" + runId + '\'' +
                ", libraryLayout='" + libraryLayout + '\'' +
                ", sampleName='" + sampleName + '\'' +
                ", srcRawMetaData='" + srcRawMetaData + '\'' +
                ", comment='" + comment + '\'' +
                '}';
    }

    public abstract static class Parser implements Serializable {

        public static enum Separation {
            TAB("\t"), COMMA(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            public String separationPattern;

            Separation(String separationPattern) {
                this.separationPattern = separationPattern;
            }
        }

        public Parser() {
            this(Separation.TAB);
        }

        public Parser(Separation separation) {
            this.separation = separation;
        }

        private Separation separation;

        public SampleMetaData parse(String csvLine) throws CsvParseException {
            String[] partsFromCsvLine = getPartsFromCsvLine(csvLine);
            return processParts(partsFromCsvLine, csvLine);
        }

        public abstract SampleMetaData processParts(String[] csvLineParts, String csvLine) throws CsvParseException;

        public String[] getPartsFromCsvLine(String csvLine) {
            String[] parts = csvLine.split(separation.separationPattern);
            for (int i = 0; i < parts.length; i++) {
                if (parts[i].length() > 0 && parts[i].charAt(0) == '"') {
                    parts[i] = parts[i].substring(1);
                }
                if (parts[i].length() > 0 && parts[i].charAt(parts[i].length() - 1) == '"') {
                    parts[i] = parts[i].substring(0, parts[i].length() - 1);
                }
            }
            return parts;
        }

        public static class CsvParseException extends RuntimeException {
            public CsvParseException(String csvLine) {
                super(String.format("Exception occurred while %s was parsing", csvLine));
            }
        }
    }
}

