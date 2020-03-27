package com.google.allenday.genomics.core.reference;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class ReferenceDatabase implements Serializable {

    private String dbName;

    private String fastaGcsUri;
    private String faiGcsUri;

    private String fastaLocalPath;

    public ReferenceDatabase() {
    }

    public ReferenceDatabase(String dbName, String fastaGcsUri, String faiGcsUri, String fastaLocalPath) {
        this.dbName = dbName;
        this.fastaGcsUri = fastaGcsUri;
        this.faiGcsUri = faiGcsUri;
        this.fastaLocalPath = fastaLocalPath;
    }

    public String getFastaGcsUri() {
        return fastaGcsUri;
    }

    public String getDbName() {
        return dbName;
    }

    public String getFaiGcsUri() {
        return faiGcsUri;
    }

    public String getFastaLocalPath() {
        return fastaLocalPath;
    }

    public static class Builder {

        private String dbName;

        private String fastaGcsUri;
        private String faiGcsUri;

        private String fastaLocalPath;

        public Builder(String dbName, String fastaGcsUri) {
            this.dbName = dbName;
            this.fastaGcsUri = fastaGcsUri;
        }

        public Builder withIndexUri(String indexUri) {
            this.faiGcsUri = indexUri;
            return this;
        }

        public Builder withfastaLocalPath(String fastaLocalPath) {
            this.fastaLocalPath = fastaLocalPath;
            return this;
        }

        public ReferenceDatabase build() {
            return new ReferenceDatabase(dbName, fastaGcsUri, faiGcsUri, fastaLocalPath);
        }

    }
}