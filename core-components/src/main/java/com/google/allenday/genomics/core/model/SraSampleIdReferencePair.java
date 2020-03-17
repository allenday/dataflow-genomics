package com.google.allenday.genomics.core.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class SraSampleIdReferencePair implements Serializable {

    private SraSampleId sraSampleId;
    private String referenceName;

    public SraSampleIdReferencePair() {
    }

    public SraSampleIdReferencePair(SraSampleId sraSampleId, String referenceName) {
        this.sraSampleId = sraSampleId;
        this.referenceName = referenceName;
    }

    public SraSampleId getSraSampleId() {
        return sraSampleId;
    }

    public String getReferenceName() {
        return referenceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SraSampleIdReferencePair that = (SraSampleIdReferencePair) o;
        return Objects.equals(sraSampleId, that.sraSampleId) &&
                Objects.equals(referenceName, that.referenceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sraSampleId, referenceName);
    }

    @Override
    public String toString() {
        return "SraSampleIdReferencePair{" +
                "sraSampleId=" + sraSampleId +
                ", referenceName='" + referenceName + '\'' +
                '}';
    }
}
