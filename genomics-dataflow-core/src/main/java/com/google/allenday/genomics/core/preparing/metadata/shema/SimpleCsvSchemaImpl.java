package com.google.allenday.genomics.core.preparing.metadata.shema;


public class SimpleCsvSchemaImpl extends SimpleCsvSchema {

    @Override
    public int getRunIdCsvIndex() {
        return 0;
    }

    @Override
    public int getSraSampleCsvIndex() {
        return 1;
    }

    @Override
    public int getSraStudyCsvIndex() {
        return 2;
    }

    @Override
    public int getLibraryLayoutCsvIndex() {
        return 3;
    }

    @Override
    public int getPlatformCsvIndex() {
        return 4;
    }

    @Override
    public int getDataSourceTypeCsvIndex() {
        return 5;
    }

    @Override
    public int getDataSourceUriCsvIndex() {
        return 6;
    }
}
