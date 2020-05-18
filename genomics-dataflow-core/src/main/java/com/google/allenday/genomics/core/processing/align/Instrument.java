package com.google.allenday.genomics.core.processing.align;

public enum Instrument {
    OXFORD_NANOPORE(1000),
    ILLUMINA(1),
    PACBIO_SMRT(100),
    LS454(1),
    MGISEQ(2);

    public final int sizeMultiplier;

    Instrument(int sizeMultiplier) {
        this.sizeMultiplier = sizeMultiplier;
    }
}