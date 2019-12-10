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
public class GeneExampleMetaData extends GeneReadGroupMetaData implements Serializable {

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

    private String runId;
    private String libraryLayout;

    @Nullable
    private String sampleName;

    private String srcRawMetaData;
    @Nullable
    private String comment;

    public GeneExampleMetaData() {
    }

    public GeneExampleMetaData(String sraSample, String runId, String libraryLayout, String srcRawMetaData) {
        super(sraSample);
        this.runId = runId;
        this.libraryLayout = libraryLayout;
        this.srcRawMetaData = srcRawMetaData;
    }

    public GeneExampleMetaData setSampleName(String sampleName) {
        this.sampleName = sampleName;
        return this;
    }

    public static GeneExampleMetaData fromCsvLine(Parser parser, String csvLine) throws Parser.CsvParseException {
        return parser.parse(csvLine);
    }

    public static GeneExampleMetaData createSingleEndUnique(String rawMetaData) {
        String uniqueName = UUID.randomUUID().toString();
        return new GeneExampleMetaData(
                "sraSample_" + uniqueName,
                "runId_" + uniqueName,
                "SINGLE",
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GeneExampleMetaData that = (GeneExampleMetaData) o;
        return Objects.equals(avgSpotLen, that.avgSpotLen) &&
                Objects.equals(datastoreProvider, that.datastoreProvider) &&
                Objects.equals(datastoreRegion, that.datastoreRegion) &&
                Objects.equals(insertSize, that.insertSize) &&
                Objects.equals(libraryName, that.libraryName) &&
                Objects.equals(numBases, that.numBases) &&
                Objects.equals(numBytes, that.numBytes) &&
                Objects.equals(runId, that.runId) &&
                Objects.equals(libraryLayout, that.libraryLayout) &&
                Objects.equals(sampleName, that.sampleName) &&
                Objects.equals(srcRawMetaData, that.srcRawMetaData) &&
                Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), avgSpotLen, datastoreProvider, datastoreRegion, insertSize, libraryName, numBases, numBytes, runId, libraryLayout, sampleName, srcRawMetaData, comment);
    }

    @Override
    public String toString() {
        return "GeneExampleMetaData{" +
                "avgSpotLen=" + avgSpotLen +
                ", datastoreProvider='" + datastoreProvider + '\'' +
                ", datastoreRegion='" + datastoreRegion + '\'' +
                ", insertSize=" + insertSize +
                ", libraryName='" + libraryName + '\'' +
                ", numBases=" + numBases +
                ", numBytes=" + numBytes +
                ", runId='" + runId + '\'' +
                ", libraryLayout='" + libraryLayout + '\'' +
                ", sampleName='" + sampleName + '\'' +
                ", srcRawMetaData='" + srcRawMetaData + '\'' +
                ", comment='" + comment + '\'' +
                ", sraSample='" + sraSample + '\'' +
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

        public GeneExampleMetaData parse(String csvLine) throws CsvParseException {
            String[] partsFromCsvLine = getPartsFromCsvLine(csvLine);
            return processParts(partsFromCsvLine, csvLine);
        }

        public abstract GeneExampleMetaData processParts(String[] csvLineParts, String csvLine) throws CsvParseException;

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

