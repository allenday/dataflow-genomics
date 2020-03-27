package com.google.allenday.genomics.core.reference;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface ReferenceDatabaseSource extends Serializable {

    public final static String[] REFERENCE_FILE_EXTENSIONS = {".fa", ".fasta"};
    public final static String INDEX_SUFFIX = ".fai";

    public abstract Optional<Blob> getReferenceBlob(GCSService gcsService, FileUtils fileUtils);

    public abstract Optional<Blob> getReferenceIndexBlob(GCSService gcsService, FileUtils fileUtils);

    public abstract Optional<Blob> getCustomDatabaseFileByKey(GCSService gcsService, String key);

    public abstract String getName();

    public static class ByNameAndUriSchema implements ReferenceDatabaseSource {

        private String name;
        private String gcsDirUri;

        public ByNameAndUriSchema() {
        }

        public ByNameAndUriSchema(String name, String gcsDirUri) {
            this.name = name;
            this.gcsDirUri = gcsDirUri;
        }

        private List<Blob> getBlobsInDir(GCSService gcsService, FileUtils fileUtils, String uri, String name) {
            BlobId blobIdFromUri = gcsService.getBlobIdFromUri(uri);

            return gcsService.getAllBlobsIn(blobIdFromUri.getBucket(), blobIdFromUri.getName())
                    .stream()
                    .filter(blob -> fileUtils.getFilenameFromPath(blob.getName()).startsWith(name))
                    .collect(Collectors.toList());
        }

        @Override
        public Optional<Blob> getReferenceBlob(GCSService gcsService, FileUtils fileUtils) {
            List<Blob> dbFiles = getBlobsInDir(gcsService, fileUtils, gcsDirUri, name);
            return dbFiles.stream().filter(blob -> Stream.of(REFERENCE_FILE_EXTENSIONS)
                    .anyMatch(ext -> blob.getName().endsWith(ext))).findFirst();
        }

        @Override
        public Optional<Blob> getReferenceIndexBlob(GCSService gcsService, FileUtils fileUtils) {
            List<Blob> dbFiles = getBlobsInDir(gcsService, fileUtils, gcsDirUri, name);
            return dbFiles.stream().filter(blob -> blob.getName().endsWith(INDEX_SUFFIX)).findFirst();
        }

        @Override
        public Optional<Blob> getCustomDatabaseFileByKey(GCSService gcsService, String key) {
            return Optional.empty();
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ByNameAndUriSchema that = (ByNameAndUriSchema) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(gcsDirUri, that.gcsDirUri);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, gcsDirUri);
        }
    }

    public static class Explicit implements ReferenceDatabaseSource {

        public final static String REF_NAME_JSON_KEY = "name";
        public final static String FASTA_URI_JSON_KEY = "fastaUri";
        public final static String INDEX_URI_JSON_KEY = "indexUri";

        private String refDataJsonString;

        public Explicit() {
        }

        public Explicit(String refDataJsonString) {
            this.refDataJsonString = refDataJsonString;
        }

        public static List<ReferenceDatabaseSource> fromRefDataJsonString(String refDataJsonArrayString) {
            JsonArray jsonArray = new JsonParser().parse(refDataJsonArrayString).getAsJsonArray();

            List<ReferenceDatabaseSource> referenceDatabaseSources = new ArrayList<>();
            for (int i = 0; i < jsonArray.size(); i++) {
                referenceDatabaseSources.add(new ReferenceDatabaseSource.Explicit(jsonArray.get(i).toString()));
            }
            return referenceDatabaseSources;
        }

        @Override
        public Optional<Blob> getReferenceBlob(GCSService gcsService, FileUtils fileUtils) {
            return getCustomDatabaseFileByKey(gcsService, FASTA_URI_JSON_KEY);
        }

        @Override
        public Optional<Blob> getReferenceIndexBlob(GCSService gcsService, FileUtils fileUtils) {
            return getCustomDatabaseFileByKey(gcsService, INDEX_URI_JSON_KEY);
        }

        @Override
        public Optional<Blob> getCustomDatabaseFileByKey(GCSService gcsService, String key) {
            return Optional.ofNullable(new JsonParser().parse(refDataJsonString).getAsJsonObject().get(key))
                    .map(JsonElement::getAsString).map(gcsService::getBlobIdFromUri)
                    .map(blobId -> Optional.ofNullable(gcsService.isExists(blobId) ? gcsService.getBlob(blobId) : null))
                    .flatMap(opt -> opt);
        }

        @Override
        public String getName() {
            return Optional.ofNullable(new JsonParser().parse(refDataJsonString).getAsJsonObject().get(REF_NAME_JSON_KEY))
                    .map(JsonElement::getAsString).map(refName -> refName.replace(" ", "_")).orElse(null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Explicit explicit = (Explicit) o;
            return Objects.equals(refDataJsonString, explicit.refDataJsonString);
        }

        @Override
        public int hashCode() {
            return Objects.hash(refDataJsonString);
        }
    }
}
