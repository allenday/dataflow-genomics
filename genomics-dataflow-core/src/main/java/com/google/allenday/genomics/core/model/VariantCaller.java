package com.google.allenday.genomics.core.model;

public enum VariantCaller {
    GATK("gatk"),
    DEEP_VARIANT("deep_variant");

    public final String arg;

    VariantCaller(String arg) {
        this.arg = arg;
    }
}
