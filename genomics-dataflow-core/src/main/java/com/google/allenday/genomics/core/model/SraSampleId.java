package com.google.allenday.genomics.core.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class SraSampleId implements Serializable {

    private String value;

    public SraSampleId() {
    }

    private SraSampleId(String value) {
        this.value = value;
    }

    public static SraSampleId create(String id){
        return new SraSampleId(id);
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SraSampleId that = (SraSampleId) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
