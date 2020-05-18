package com.google.allenday.genomics.core.preparing.runfile;

import java.io.Serializable;

public class SraInputResource implements Serializable {

    private String sraAccession;

    public SraInputResource(String sraAccession) {
        this.sraAccession = sraAccession;
    }
}
