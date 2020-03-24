package com.google.allenday.genomics.core.model;

public enum Instrument {
    OXFORD_NANOPORE("map-ont", 1000),
    ILLUMINA("sr", 1),
    PACBIO_SMRT("map-pb", 100),
    LS454("sr", 1000);

    public final String flag;
    public final int sizeMultiplier;

    Instrument(String flag, int sizeMultiplier) {
        this.flag = flag;
        this.sizeMultiplier = sizeMultiplier;
    }
}