package com.google.allenday.genomics.core.preparing.metadata.shema;


import java.io.Serializable;

public abstract class CsvSchema implements Serializable {

    public abstract int getAvgSpotLenCsvIndex();

    public abstract int getBioSampleCsvIndex();

    public abstract int getDatastoreProviderCsvIndex();

    public abstract int getDatastoreRegionCsvIndex();

    public abstract int getExperimentCsvIndex();

    public abstract int getInsertSizeCsvIndex();

    public abstract int getLibraryLayoutCsvIndex();

    public abstract int getLibratyNameCsvIndex();

    public abstract int getNumBasesCsvIndex();

    public abstract int getNumBytesCsvIndex();

    public abstract int getRunIdCsvIndex();

    public abstract int getSraSampleCsvIndex();

    public abstract int getSampleNameCsvIndex();

    public abstract int getAssayTypeCsvIndex();

    public abstract int getBioProjectCsvIndex();

    public abstract int getCenterNameCsvIndex();

    public abstract int getConsentCsvIndex();

    public abstract int getDatastoreFiletypeCsvIndex();

    public abstract int getInstrumentCsvIndex();

    public abstract int getLibrarySelectionCsvIndex();

    public abstract int getLibrarySourceCsvIndex();

    public abstract int getLoadDateCsvIndex();

    public abstract int getOrganismCsvIndex();

    public abstract int getPlatformCsvIndex();

    public abstract int getReleaseDateCsvIndex();

    public abstract int getSraStudyCsvIndex();

    public abstract int getDataSourceTypeCsvIndex();

    public abstract int getDataSourceUriCsvIndex();
}
