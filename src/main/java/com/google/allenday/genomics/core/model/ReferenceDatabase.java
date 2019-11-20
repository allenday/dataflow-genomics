package com.google.allenday.genomics.core.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class ReferenceDatabase implements Serializable {

    private final static String INDEX_SUFFIX = ".fai";

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

    public Pair<String, String> getRefUriWithIndex(String refExtension) {
        String ref = dbFilesUris.stream().filter(f -> f.endsWith(refExtension)).findFirst().orElse(null);
        String refIndex = dbFilesUris.stream().filter(f -> f.endsWith(refExtension + INDEX_SUFFIX)).findFirst().orElse(null);
        return Pair.with(ref, refIndex);
    }
}