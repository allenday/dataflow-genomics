package com.google.allenday.genomics.core.model;

import java.io.Serializable;

public class BamWithIndexUris implements Serializable {

    private String bamUri;
    private String indexUri;

    public BamWithIndexUris(String bamUri, String indexUri) {
        this.bamUri = bamUri;
        this.indexUri = indexUri;
    }

    public String getBamUri() {
        return bamUri;
    }

    public String getIndexUri() {
        return indexUri;
    }

    @Override
    public String toString() {
        return "BamWithIndexUris{" +
                "bamUri='" + bamUri + '\'' +
                ", indexUri='" + indexUri + '\'' +
                '}';
    }
}
