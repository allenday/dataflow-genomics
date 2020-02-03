package com.google.allenday.genomics.core.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class SraSampleIdReferencePair implements Serializable {

    private SraSampleId sraSampleId;
    private ReferenceDatabase referenceDatabase;

    public SraSampleIdReferencePair() {
    }

    public SraSampleIdReferencePair(SraSampleId sraSampleId, ReferenceDatabase referenceDatabase) {
        this.sraSampleId = sraSampleId;
        this.referenceDatabase = referenceDatabase;
    }

    public SraSampleId getSraSampleId() {
        return sraSampleId;
    }

    public void setSraSampleId(SraSampleId sraSampleId) {
        this.sraSampleId = sraSampleId;
    }

    public ReferenceDatabase getReferenceDatabase() {
        return referenceDatabase;
    }

    public void setReferenceDatabase(ReferenceDatabase referenceDatabase) {
        this.referenceDatabase = referenceDatabase;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SraSampleIdReferencePair that = (SraSampleIdReferencePair) o;
        return Objects.equals(sraSampleId, that.sraSampleId) &&
                Objects.equals(referenceDatabase, that.referenceDatabase);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sraSampleId, referenceDatabase);
    }

    @Override
    public String toString() {
        return "SraSampleIdReferencePair{" +
                "sraSampleId=" + sraSampleId +
                ", referenceDatabase=" + referenceDatabase +
                '}';
    }
}
