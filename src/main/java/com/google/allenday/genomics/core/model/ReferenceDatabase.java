package com.google.allenday.genomics.core.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class ReferenceDatabase implements Serializable {

    private String dbName;
    private List<String> dbFilesUris;

    public ReferenceDatabase() {
    }

    public ReferenceDatabase(String dbName, List<String> dbFilesUris) {
        this.dbName = dbName;
        this.dbFilesUris = dbFilesUris;
    }

    public String getDbName() {
        return dbName;
    }

    public List<String> getDbFilesUris() {
        return dbFilesUris;
    }
}
