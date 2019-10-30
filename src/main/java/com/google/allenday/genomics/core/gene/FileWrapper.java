package com.google.allenday.genomics.core.gene;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class FileWrapper implements Serializable {

    private DataType dataType;
    private String fileName;
    @Nullable
    private String blobUri;
    @Nullable
    private byte[] content;

    private FileWrapper(DataType dataType, String fileName) {
        this.dataType = dataType;
        this.fileName = fileName;
    }

    public static FileWrapper fromBlobUri(String blobUri, String fileName){
        return new FileWrapper(DataType.BLOB_URI, fileName).withBlobUri(blobUri);
    }

    public static FileWrapper fromByteArrayContent(byte[] content, String fileName){
        return new FileWrapper(DataType.CONTENT, fileName).withContent(content);
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

    public FileWrapper withBlobUri(String blobUri) {
        this.blobUri = blobUri;
        return this;
    }

    public FileWrapper withContent(byte[] raw) {
        this.content = raw;
        return this;
    }

    @DefaultCoder(SerializableCoder.class)
    public enum DataType {
        CONTENT, BLOB_URI
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileWrapper fileWrapper = (FileWrapper) o;
        return dataType == fileWrapper.dataType &&
                Objects.equals(fileName, fileWrapper.fileName) &&
                Objects.equals(blobUri, fileWrapper.blobUri) &&
                Arrays.equals(content, fileWrapper.content);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(dataType, fileName, blobUri);
        result = 31 * result + Arrays.hashCode(content);
        return result;
    }

    @Override
    public String toString() {
        return "FileWrapper{" +
                "dataType=" + dataType +
                ", fileName='" + fileName + '\'' +
                ", blobUri='" + blobUri + '\'' +
                ", content=" + Arrays.toString(content) +
                '}';
    }
}
