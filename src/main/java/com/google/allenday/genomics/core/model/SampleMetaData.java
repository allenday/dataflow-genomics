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
public class SampleMetaData extends ReadGroupMetaData implements Serializable {

    private final static String IS_PAIRED_FLAG = "PAIRED";

    private String runId;
    private String libraryLayout;

    @Nullable
    private String sampleName;

    private String srcRawMetaData;
    @Nullable
    private String comment;

    public SampleMetaData() {
    }

    public SampleMetaData(String sraSample, String runId, String libraryLayout, String srcRawMetaData) {
        super(sraSample);
        this.runId = runId;
        this.libraryLayout = libraryLayout;
        this.srcRawMetaData = srcRawMetaData;
    }

    public String getRunId() {
        return runId;
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

    public boolean isPaired() {
        return libraryLayout.equals(IS_PAIRED_FLAG);
    }

    public String getLibraryLayout() {
        return libraryLayout;
    }

    public String getSampleName() {
        return sampleName;
    }

    public SampleMetaData setSampleName(String sampleName) {
        this.sampleName = sampleName;
        return this;
    }

    public static SampleMetaData fromCsvLine(Parser parser, String csvLine) throws Parser.CsvParseException {
        return parser.parse(csvLine);
    }

    public static SampleMetaData createSingleEndUnique(String rawMetaData) {
        String uniqueName = UUID.randomUUID().toString();
        return new SampleMetaData(
                "sraSample_" + uniqueName,
                "runId_" + uniqueName,
                "SINGLE",
                rawMetaData);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        SampleMetaData that = (SampleMetaData) o;
        return Objects.equals(runId, that.runId) &&
                Objects.equals(libraryLayout, that.libraryLayout) &&
                Objects.equals(sampleName, that.sampleName) &&
                Objects.equals(srcRawMetaData, that.srcRawMetaData) &&
                Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), runId, libraryLayout, sampleName, srcRawMetaData, comment);
    }

    @Override
    public String toString() {
        return "SampleMetaData{" +
                "runId='" + runId + '\'' +
                ", libraryLayout='" + libraryLayout + '\'' +
                ", sampleName='" + sampleName + '\'' +
                ", srcRawMetaData='" + srcRawMetaData + '\'' +
                ", comment='" + comment + '\'' +
                ", avgSpotLen=" + avgSpotLen +
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

    public abstract static class Parser implements Serializable {

        public SampleMetaData parse(String csvLine) throws CsvParseException {
            String[] partsFromCsvLine = getPartsFromCsvLine(csvLine);
            return processParts(partsFromCsvLine, csvLine);
        }

        public abstract SampleMetaData processParts(String[] csvLineParts, String csvLine) throws CsvParseException;

        public String[] getPartsFromCsvLine(String csvLine) {
            String[] parts = csvLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
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

