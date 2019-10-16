package com.google.allenday.genomics.core.gene;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class GeneData implements Serializable {

    private DataType dataType;
    private String fileName;
    @Nullable
    private String blobUri;
    @Nullable
    private byte[] content;
    @Nullable
    private String referenceName;

    private GeneData(DataType dataType, String fileName) {
        this.dataType = dataType;
        this.fileName = fileName;
    }

    public static GeneData fromBlobUri(String blobUri, String fileName){
        return new GeneData(DataType.BLOB_URI, fileName).withBlobUri(blobUri);
    }

    public static GeneData fromByteArrayContent(byte[] content, String fileName){
        return new GeneData(DataType.CONTENT, fileName).withContent(content);
    }

    @Nullable
    public String getBlobUri() {
        return blobUri;
    }

    @Nullable
    public byte[] getContent() {
        return content;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getFileName() {
        return fileName;
    }

    public GeneData withBlobUri(String blobUri) {
        this.blobUri = blobUri;
        return this;
    }

    public GeneData withContent(byte[] raw) {
        this.content = raw;
        return this;
    }

    public GeneData withReferenceName(String referenceName) {
        this.referenceName = referenceName;
        return this;
    }

    @Nullable
    public String getReferenceName() {
        return referenceName;
    }

    @DefaultCoder(SerializableCoder.class)
    public enum DataType {
        CONTENT, BLOB_URI
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeneData geneData = (GeneData) o;
        return dataType == geneData.dataType &&
                Objects.equals(fileName, geneData.fileName) &&
                Objects.equals(blobUri, geneData.blobUri) &&
                Arrays.equals(content, geneData.content) &&
                Objects.equals(referenceName, geneData.referenceName);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(dataType, fileName, blobUri, referenceName);
        result = 31 * result + Arrays.hashCode(content);
        return result;
    }

    @Override
    public String toString() {
        return "GeneData{" +
                "dataType=" + dataType +
                ", fileName='" + fileName + '\'' +
                ", blobUri='" + blobUri + '\'' +
                ", rawSize=" + (content != null ? String.valueOf(content.length) : "0") +
                ", referenceName='" + referenceName + '\'' +
                '}';
    }
}
