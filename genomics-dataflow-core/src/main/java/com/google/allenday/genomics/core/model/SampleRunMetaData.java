package com.google.allenday.genomics.core.model;

import com.google.allenday.genomics.core.preparing.metadata.shema.CsvSchema;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.validator.routines.IntegerValidator;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Data class that represents metadata for each sequence run
 */
@DefaultCoder(AvroCoder.class)
public class SampleRunMetaData implements Serializable {

    private String runId;
    private SraSampleId sraSample;
    @Nullable
    private String sraStudy;
    private String libraryLayout;
    private String platform;
    private DataSource dataSource;

    @Nullable
    private AdditionalMetaData additionalMetaData;
    @Nullable
    private String rawMetaData;
    @Nullable
    private String comment;

    private Integer partIndex = -1;
    private Integer subPartIndex = 0;

    public SampleRunMetaData() {
    }

    public SampleRunMetaData(String sraSample, String runId, String libraryLayout, String platform, DataSource dataSource) {
        this.sraSample = SraSampleId.create(sraSample);
        this.runId = runId;
        this.libraryLayout = libraryLayout;
        this.platform = platform;
        this.dataSource = dataSource;
    }

    public static SampleRunMetaData fromCsvLine(Parser parser, String csvLine) throws IOException {
        return parser.parse(csvLine);
    }

    public static SampleRunMetaData createUnique(String rawMetaData, String libraryLayout, String platform, DataSource dataSource) {
        String uniqueName = UUID.randomUUID().toString();
        SampleRunMetaData sampleRunMetaData = new SampleRunMetaData(
                "sraSample_" + uniqueName,
                "runId_" + uniqueName,
                libraryLayout,
                platform,
                dataSource
        );
        sampleRunMetaData.setRawMetaData(rawMetaData);
        return sampleRunMetaData;
    }


    public SampleRunMetaData(String runId, SraSampleId sraSample, String sraStudy, String libraryLayout,
                             String platform, DataSource dataSource, AdditionalMetaData additionalMetaData,
                             String rawMetaData, String comment, Integer partIndex, Integer subPartIndex) {
        this.runId = runId;
        this.sraSample = sraSample;
        this.sraStudy = sraStudy;
        this.libraryLayout = libraryLayout;
        this.platform = platform;
        this.dataSource = dataSource;
        this.additionalMetaData = additionalMetaData;
        this.rawMetaData = rawMetaData;
        this.comment = comment;
        this.partIndex = partIndex;
        this.subPartIndex = subPartIndex;
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

    public String getRawMetaData() {
        return rawMetaData;
    }

    public void setRawMetaData(String rawMetaData) {
        this.rawMetaData = rawMetaData;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public boolean isPaired() {
        return libraryLayout.equals(LibraryLayout.PAIRED.name());
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public SraSampleId getSraSample() {
        return sraSample;
    }

    public void setSraSample(SraSampleId sraSample) {
        this.sraSample = sraSample;
    }

    public String getSraStudy() {
        return sraStudy;
    }

    public void setSraStudy(String sraStudy) {
        this.sraStudy = sraStudy;
    }

    public Integer getPartIndex() {
        return partIndex;
    }

    public void setPartIndex(Integer partIndex) {
        this.partIndex = partIndex;
    }

    public Integer getSubPartIndex() {
        return subPartIndex;
    }

    public void setSubPartIndex(Integer subPartIndex) {
        this.subPartIndex = subPartIndex;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public AdditionalMetaData getAdditionalMetaData() {
        return additionalMetaData;
    }

    public void setAdditionalMetaData(AdditionalMetaData additionalMetaData) {
        this.additionalMetaData = additionalMetaData;
    }

    private SampleRunMetaData copy() {
        SampleRunMetaData sampleRunMetaData = new SampleRunMetaData();
        sampleRunMetaData.setRunId(runId);
        sampleRunMetaData.setSraSample(SraSampleId.create(sraSample.getValue()));
        sampleRunMetaData.setSraStudy(sraStudy);
        sampleRunMetaData.setLibraryLayout(libraryLayout);
        sampleRunMetaData.setPlatform(platform);
        sampleRunMetaData.setDataSource(new DataSource(dataSource.type, new ArrayList<>(dataSource.uris)));
        sampleRunMetaData.setAdditionalMetaData(additionalMetaData.copy());
        sampleRunMetaData.setRawMetaData(rawMetaData);
        sampleRunMetaData.setComment(comment);
        sampleRunMetaData.setPartIndex(partIndex);
        sampleRunMetaData.setSubPartIndex(subPartIndex);
        return sampleRunMetaData;
    }

    public SampleRunMetaData cloneWithNewPartIndex(int partIndex) {
        SampleRunMetaData sampleRunMetaDataCopy = copy();
        sampleRunMetaDataCopy.setPartIndex(partIndex);
        return sampleRunMetaDataCopy;
    }

    public SampleRunMetaData cloneWithNewSubPartIndex(int subPartIndex) {
        SampleRunMetaData sampleRunMetaDataCopy = copy();
        sampleRunMetaDataCopy.setSubPartIndex(subPartIndex);
        return sampleRunMetaDataCopy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleRunMetaData that = (SampleRunMetaData) o;
        return Objects.equals(runId, that.runId) &&
                Objects.equals(sraSample, that.sraSample) &&
                Objects.equals(sraStudy, that.sraStudy) &&
                Objects.equals(libraryLayout, that.libraryLayout) &&
                Objects.equals(platform, that.platform) &&
                Objects.equals(dataSource, that.dataSource) &&
                Objects.equals(additionalMetaData, that.additionalMetaData) &&
                Objects.equals(rawMetaData, that.rawMetaData) &&
                Objects.equals(comment, that.comment) &&
                Objects.equals(partIndex, that.partIndex) &&
                Objects.equals(subPartIndex, that.subPartIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runId, sraSample, sraStudy, libraryLayout, platform, dataSource, additionalMetaData, rawMetaData, comment, partIndex, subPartIndex);
    }

    @Override
    public String toString() {
        return "SampleRunMetaData{" +
                "runId='" + runId + '\'' +
                ", sraSample=" + sraSample +
                ", sraStudy='" + sraStudy + '\'' +
                ", libraryLayout='" + libraryLayout + '\'' +
                ", platform='" + platform + '\'' +
                ", dataSource=" + dataSource +
                ", additionalMetaData=" + additionalMetaData +
                ", rawMetaData='" + rawMetaData + '\'' +
                ", comment='" + comment + '\'' +
                ", partIndex=" + partIndex +
                ", subPartIndex=" + subPartIndex +
                '}';
    }

    @DefaultCoder(AvroCoder.class)
    public static class AdditionalMetaData implements Serializable {

        @Nullable
        private Integer avgSpotLen;
        @Nullable
        private String datastoreProvider;
        @Nullable
        private String datastoreRegion;
        @Nullable
        private Integer insertSize;
        @Nullable
        private String libraryName;
        @Nullable
        private Integer numBases;
        @Nullable
        private Integer numBytes;
        @Nullable
        private String experiment;
        @Nullable
        private String instrument;
        @Nullable
        private String loadDate;
        @Nullable
        private String releaseDate;
        @Nullable
        private String assayType;
        @Nullable
        private String centerName;
        @Nullable
        private String librarySelection;
        @Nullable
        private String librarySource;
        @Nullable
        private String datastoreFiletype;
        @Nullable
        private String bioSample;
        @Nullable
        private String bioProject;
        @Nullable
        private String consent;
        @Nullable
        private String organism;
        @Nullable
        private String sampleName;

        public AdditionalMetaData() {
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

        public String getSampleName() {
            return sampleName;
        }

        public void setSampleName(String sampleName) {
            this.sampleName = sampleName;
        }

        public AdditionalMetaData copy() {
            AdditionalMetaData additionalMetaData = new AdditionalMetaData();
            additionalMetaData.setAvgSpotLen(avgSpotLen);
            additionalMetaData.setDatastoreProvider(datastoreProvider);
            additionalMetaData.setDatastoreRegion(datastoreRegion);
            additionalMetaData.setInsertSize(insertSize);
            additionalMetaData.setLibraryName(libraryName);
            additionalMetaData.setNumBases(numBases);
            additionalMetaData.setNumBytes(numBytes);
            additionalMetaData.setExperiment(experiment);
            additionalMetaData.setInstrument(instrument);
            additionalMetaData.setLoadDate(loadDate);
            additionalMetaData.setReleaseDate(releaseDate);
            additionalMetaData.setAssayType(assayType);
            additionalMetaData.setCenterName(centerName);
            additionalMetaData.setLibraryName(librarySelection);
            additionalMetaData.setLibrarySource(librarySource);
            additionalMetaData.setDatastoreFiletype(datastoreFiletype);
            additionalMetaData.setBioSample(bioSample);
            additionalMetaData.setBioProject(bioProject);
            additionalMetaData.setConsent(consent);
            additionalMetaData.setOrganism(organism);
            additionalMetaData.setSampleName(sampleName);
            return additionalMetaData;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AdditionalMetaData that = (AdditionalMetaData) o;
            return Objects.equals(avgSpotLen, that.avgSpotLen) &&
                    Objects.equals(datastoreProvider, that.datastoreProvider) &&
                    Objects.equals(datastoreRegion, that.datastoreRegion) &&
                    Objects.equals(insertSize, that.insertSize) &&
                    Objects.equals(libraryName, that.libraryName) &&
                    Objects.equals(numBases, that.numBases) &&
                    Objects.equals(numBytes, that.numBytes) &&
                    Objects.equals(experiment, that.experiment) &&
                    Objects.equals(instrument, that.instrument) &&
                    Objects.equals(loadDate, that.loadDate) &&
                    Objects.equals(releaseDate, that.releaseDate) &&
                    Objects.equals(assayType, that.assayType) &&
                    Objects.equals(centerName, that.centerName) &&
                    Objects.equals(librarySelection, that.librarySelection) &&
                    Objects.equals(librarySource, that.librarySource) &&
                    Objects.equals(datastoreFiletype, that.datastoreFiletype) &&
                    Objects.equals(bioSample, that.bioSample) &&
                    Objects.equals(bioProject, that.bioProject) &&
                    Objects.equals(consent, that.consent) &&
                    Objects.equals(organism, that.organism) &&
                    Objects.equals(sampleName, that.sampleName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(avgSpotLen, datastoreProvider, datastoreRegion, insertSize, libraryName, numBases, numBytes, experiment, instrument, loadDate, releaseDate, assayType, centerName, librarySelection, librarySource, datastoreFiletype, bioSample, bioProject, consent, organism, sampleName);
        }

        @Override
        public String toString() {
            return "AdditionalMetaData{" +
                    "avgSpotLen=" + avgSpotLen +
                    ", datastoreProvider='" + datastoreProvider + '\'' +
                    ", datastoreRegion='" + datastoreRegion + '\'' +
                    ", insertSize=" + insertSize +
                    ", libraryName='" + libraryName + '\'' +
                    ", numBases=" + numBases +
                    ", numBytes=" + numBytes +
                    ", experiment='" + experiment + '\'' +
                    ", instrument='" + instrument + '\'' +
                    ", loadDate='" + loadDate + '\'' +
                    ", releaseDate='" + releaseDate + '\'' +
                    ", assayType='" + assayType + '\'' +
                    ", centerName='" + centerName + '\'' +
                    ", librarySelection='" + librarySelection + '\'' +
                    ", librarySource='" + librarySource + '\'' +
                    ", datastoreFiletype='" + datastoreFiletype + '\'' +
                    ", bioSample='" + bioSample + '\'' +
                    ", bioProject='" + bioProject + '\'' +
                    ", consent='" + consent + '\'' +
                    ", organism='" + organism + '\'' +
                    ", sampleName='" + sampleName + '\'' +
                    '}';
        }
    }

    public static class Parser implements Serializable {

        private final static Separation DEFAULT_SEPARATION = Separation.TAB;

        private Separation separation;
        private CsvSchema csvSchema;

        public Parser(CsvSchema csvSchema) {
            this(csvSchema, DEFAULT_SEPARATION);
        }

        public Parser(CsvSchema csvSchema, Separation separation) {
            this.separation = separation;
            this.csvSchema = csvSchema;
        }


        public SampleRunMetaData parse(String csvLine) throws IOException {
            String[] partsFromCsvLine = getPartsFromCsvLine(csvLine);
            SampleRunMetaData sampleRunMetaData = buildSampleMetaDataFromParts(partsFromCsvLine);
            sampleRunMetaData.setRawMetaData(csvLine);
            return sampleRunMetaData;
        }

        private SampleRunMetaData buildSampleMetaDataFromParts(String[] csvLineParts) throws IOException {
            try {
                SampleRunMetaData geneSampleRunMetaData = new SampleRunMetaData();
                geneSampleRunMetaData.setRunId(parseSafe(csvLineParts, csvSchema.getRunIdCsvIndex()));
                geneSampleRunMetaData.setSraSample(
                        SraSampleId.create(parseSafe(csvLineParts, csvSchema.getSraSampleCsvIndex())));
                geneSampleRunMetaData.setSraStudy(parseSafe(csvLineParts, csvSchema.getSraStudyCsvIndex()));
                geneSampleRunMetaData.setLibraryLayout(parseSafe(csvLineParts, csvSchema.getLibraryLayoutCsvIndex()));
                geneSampleRunMetaData.setPlatform(parseSafe(csvLineParts, csvSchema.getPlatformCsvIndex()));

                geneSampleRunMetaData.setAdditionalMetaData(buildAdditionalMetaDataFromSraCsvLineParts(csvLineParts));
                geneSampleRunMetaData.setDataSource(buildDataSourceFromSraCsvLineParts(csvLineParts));

                return geneSampleRunMetaData;
            } catch (Exception e) {
                CsvParseException csvParseException = new CsvParseException(String.join(",", csvLineParts));
                csvParseException.initCause(e);
                throw csvParseException;
            }
        }

        private SampleRunMetaData.DataSource buildDataSourceFromSraCsvLineParts(String[] csvLineParts)
                throws IOException {
            String typeStr = parseSafe(csvLineParts, csvSchema.getDataSourceTypeCsvIndex());
            String uriStr = parseSafe(csvLineParts, csvSchema.getDataSourceUriCsvIndex());
            return SampleRunMetaData.DataSource.create(typeStr, uriStr);
        }

        private SampleRunMetaData.AdditionalMetaData buildAdditionalMetaDataFromSraCsvLineParts(String[] csvLineParts) {
            SampleRunMetaData.AdditionalMetaData additionalMetaData = new SampleRunMetaData.AdditionalMetaData();
            additionalMetaData.setAvgSpotLen(
                    IntegerValidator.getInstance().validate(parseSafe(csvLineParts, csvSchema.getAvgSpotLenCsvIndex())));
            additionalMetaData.setBioSample(parseSafe(csvLineParts, csvSchema.getBioSampleCsvIndex()));
            additionalMetaData.setDatastoreProvider(parseSafe(csvLineParts, csvSchema.getDatastoreProviderCsvIndex()));
            additionalMetaData.setDatastoreRegion(parseSafe(csvLineParts, csvSchema.getDatastoreRegionCsvIndex()));
            additionalMetaData.setExperiment(parseSafe(csvLineParts, csvSchema.getExperimentCsvIndex()));
            additionalMetaData.setInsertSize(
                    IntegerValidator.getInstance().validate(parseSafe(csvLineParts, csvSchema.getInsertSizeCsvIndex())));
            additionalMetaData.setLibraryName(parseSafe(csvLineParts, csvSchema.getLibratyNameCsvIndex()));
            additionalMetaData.setNumBases(
                    IntegerValidator.getInstance().validate(parseSafe(csvLineParts, csvSchema.getNumBasesCsvIndex())));
            additionalMetaData.setNumBytes(
                    IntegerValidator.getInstance().validate(parseSafe(csvLineParts, csvSchema.getNumBytesCsvIndex())));
            additionalMetaData.setSampleName(parseSafe(csvLineParts, csvSchema.getSampleNameCsvIndex()));
            additionalMetaData.setAssayType(parseSafe(csvLineParts, csvSchema.getAssayTypeCsvIndex()));
            additionalMetaData.setBioProject(parseSafe(csvLineParts, csvSchema.getBioProjectCsvIndex()));
            additionalMetaData.setCenterName(parseSafe(csvLineParts, csvSchema.getCenterNameCsvIndex()));
            additionalMetaData.setConsent(parseSafe(csvLineParts, csvSchema.getConsentCsvIndex()));
            additionalMetaData.setDatastoreFiletype(parseSafe(csvLineParts, csvSchema.getDatastoreFiletypeCsvIndex()));
            additionalMetaData.setInstrument(parseSafe(csvLineParts, csvSchema.getInstrumentCsvIndex()));
            additionalMetaData.setLibrarySelection(parseSafe(csvLineParts, csvSchema.getLibrarySelectionCsvIndex()));
            additionalMetaData.setLibrarySource(parseSafe(csvLineParts, csvSchema.getLibrarySourceCsvIndex()));
            additionalMetaData.setLoadDate(parseSafe(csvLineParts, csvSchema.getLoadDateCsvIndex()));
            additionalMetaData.setOrganism(parseSafe(csvLineParts, csvSchema.getOrganismCsvIndex()));
            additionalMetaData.setReleaseDate(parseSafe(csvLineParts, csvSchema.getReleaseDateCsvIndex()));
            return additionalMetaData;
        }

        private String parseSafe(String[] csvLineParts, int index) {
            return index >= 0 && csvLineParts.length > index
                    ? csvLineParts[index] : null;
        }

        private String[] getPartsFromCsvLine(String csvLine) {
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

        public static enum Separation {
            TAB("\t"),
            COMMA(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            public String separationPattern;

            Separation(String separationPattern) {
                this.separationPattern = separationPattern;
            }
        }

        static class CsvParseException extends IOException {
            CsvParseException(String csvLine) {
                super(String.format("Exception occurred while %s was parsing", csvLine));
            }
        }
    }

    public static enum LibraryLayout {
        SINGLE, PAIRED
    }

    public static class DataSource implements Serializable {
        private Type type;
        private List<String> uris;

        public DataSource() {
        }


        public static DataSource sra() {
            return new DataSource(Type.SRA, Collections.emptyList());
        }

        static DataSource uriProvider() {
            return new DataSource(Type.GCS_URI_PROVIDER, Collections.emptyList());
        }

        public static DataSource create(String typeStr, String uriStr) throws DataSourceParseException {
            if (typeStr == null) {
                return DataSource.sra();
            } else {
                try {
                    Type type = Type.valueOf(typeStr.toUpperCase());
                    if (type == Type.SRA) {
                        return DataSource.sra();
                    } else if (type == Type.GCS_URI_PROVIDER) {
                        return DataSource.uriProvider();
                    } else if ((type == Type.URL || type == Type.GCS) && uriStr != null) {
                        return new DataSource(type,
                                Stream.of(uriStr.split(",")).map(String::trim).collect(Collectors.toList()));
                    } else {
                        throw new DataSourceParseException(typeStr, uriStr);
                    }
                } catch (IllegalArgumentException e) {
                    DataSourceParseException dataSourceParseException = new DataSourceParseException(typeStr, uriStr);
                    dataSourceParseException.initCause(e);
                    throw dataSourceParseException;
                }
            }
        }

        public DataSource(Type type, List<String> uris) {
            this.type = type;
            this.uris = uris;
        }

        public Type getType() {
            return type;
        }

        public List<String> getUris() {
            return uris;
        }


        public enum Type implements Serializable {
            URL, GCS, GCS_URI_PROVIDER, SRA;

            public static List<Type> onlySra() {
                return Collections.singletonList(SRA);
            }


            public static List<Type> allExceptSra() {
                List<Type> types = new ArrayList<>(Arrays.asList(Type.values()));
                types.remove(SRA);
                return types;
            }
        }

        public static class DataSourceParseException extends IOException {
            public DataSourceParseException(String typeStr, String uri) {
                super(String.format("Wrong data for creating DataSource: %s, %s", typeStr, uri));
            }

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataSource that = (DataSource) o;
            return type == that.type &&
                    Objects.equals(uris, that.uris);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, uris);
        }

        @Override
        public String toString() {
            return "DataSource{" +
                    "type=" + type +
                    ", uris=" + uris +
                    '}';
        }
    }
}

