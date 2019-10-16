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

    private String runId;
    private boolean isPaired;
    private String srcRawMetaData;
    private String comment = "";

    public GeneExampleMetaData() {
    }

    public GeneExampleMetaData(String project, String projectId, String bioSample, String sraSample, String runId,
                               boolean isPaired, String srcRawMetaData) {
        super(project, projectId, bioSample, sraSample);
        this.runId = runId;
        this.isPaired = isPaired;
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
        return isPaired;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GeneExampleMetaData that = (GeneExampleMetaData) o;
        return Objects.equals(runId, that.runId) &&
                Objects.equals(srcRawMetaData, that.srcRawMetaData);
    }

    public static GeneExampleMetaData fromCsvLine(Parser parser, String csvLine) {
        return parser.parse(csvLine);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), runId, srcRawMetaData);
    }

    @Override
    public String toString() {
        return "GeneExampleMetaData{" +
                "runId='" + runId + '\'' +
                ", srcRawMetaData='" + srcRawMetaData + '\'' +
                ", projectName='" + projectName + '\'' +
                ", projectId='" + projectId + '\'' +
                ", bioSample='" + bioSample + '\'' +
                ", sraSample='" + sraSample + '\'' +
                '}';
    }

    public static class Parser implements Serializable {

        private final static String IS_PAIRED_FLAG = "PAIRED";

        private final static int DEFAULT_PROJECT_NAME_COLUMN_NUM = 0;
        private final static int DEFAULT_PROJECT_ID_COLUMN_NUM = 1;
        private final static int DEFAULT_BIO_SAMPLE_COLUMN_NUM = 2;
        private final static int DEFAULT_SRA_SAMPLE_COLUMN_NUM = 3;
        private final static int DEFAULT_RUN_ID_COLUMN_NUM = 4;
        private final static int DEFAULT_IS_PAIRED_COLUMN_NUM = 6;

        private int projectNameCoulumnNum;
        private int projectIdCoulumnNum;
        private int bioSampleCoulumnNum;
        private int sraSampleCoulumnNum;
        private int runIdCoulumnNum;
        private int isPairedCoulumnNum;

        public Parser(int projectNameCoulumnNum, int projectIdCoulumnNum, int bioSampleCoulumnNum,
                      int sraSampleCoulumnNum, int runIdCoulumnNum, int isPairedCoulumnNum) {
            this.projectNameCoulumnNum = projectNameCoulumnNum;
            this.projectIdCoulumnNum = projectIdCoulumnNum;
            this.bioSampleCoulumnNum = bioSampleCoulumnNum;
            this.sraSampleCoulumnNum = sraSampleCoulumnNum;
            this.runIdCoulumnNum = runIdCoulumnNum;
            this.isPairedCoulumnNum = isPairedCoulumnNum;
        }

        public static Parser withDefaultSchema() {
            return new Parser(DEFAULT_PROJECT_NAME_COLUMN_NUM, DEFAULT_PROJECT_ID_COLUMN_NUM,
                    DEFAULT_BIO_SAMPLE_COLUMN_NUM, DEFAULT_SRA_SAMPLE_COLUMN_NUM, DEFAULT_RUN_ID_COLUMN_NUM,
                    DEFAULT_IS_PAIRED_COLUMN_NUM);
        }

        public static Parser withCustomSchema(int projectNameCoulumnNum, int projectIdCoulumnNum,
                                              int bioSampleCoulumnNum, int sraSampleCoulumnNum, int runIdCoulumnNum,
                                              int isPairedCoulumnNum) {
            return new Parser(projectNameCoulumnNum, projectIdCoulumnNum, bioSampleCoulumnNum, sraSampleCoulumnNum,
                    runIdCoulumnNum, isPairedCoulumnNum);
        }

        public GeneExampleMetaData parse(String csvLine) {
            String[] parts = csvLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            boolean isPaired = parts[isPairedCoulumnNum].toLowerCase().equals(IS_PAIRED_FLAG.toLowerCase());
            return new GeneExampleMetaData(parts[projectNameCoulumnNum], parts[projectIdCoulumnNum],
                    parts[bioSampleCoulumnNum], parts[sraSampleCoulumnNum], parts[runIdCoulumnNum],
                    isPaired, csvLine);
        }
    }
}

