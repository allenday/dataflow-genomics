package com.google.allenday.genomics.core.model;

public enum Aligner {
    MINIMAP2("minimap2"),
    BWA("bwa");

    public final String arg;

    Aligner(String arg) {
        this.arg = arg;
    }
}
