package com.google.allenday.genomics.core.reference;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.cloud.storage.BlobId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ReferencesProvider implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(ReferencesProvider.class);

    private static final String DEFAULT_REFERENCE_FILE_EXTENSION = ".fa";
    private static final String DEFAULT_ALL_REFERENCE_LOCAL_DIR = "reference/";

    private FileUtils fileUtils;
    private String allReferencesDirGcsUri;
    private String allReferencesLocalDir;
    private String referenceFileExtension;

    public ReferencesProvider(FileUtils fileUtils, String allReferencesDirGcsUri) {
        this(fileUtils, allReferencesDirGcsUri, DEFAULT_ALL_REFERENCE_LOCAL_DIR, DEFAULT_REFERENCE_FILE_EXTENSION);
    }

    public ReferencesProvider(FileUtils fileUtils, String allReferencesDirGcsUri, String allReferencesLocalDir, String referenceFileExtension) {
        this.fileUtils = fileUtils;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
        this.allReferencesLocalDir = allReferencesLocalDir;
        this.referenceFileExtension = referenceFileExtension;
    }

    public String findReference(GCSService gcsService, String referenceName) {
        BlobId blobIdFromUri = gcsService.getBlobIdFromUri(allReferencesDirGcsUri);
        gcsService.getAllBlobsIn(blobIdFromUri.getBucket(), blobIdFromUri.getName())
                .stream()
                .filter(blob -> blob.getName().contains(referenceName))
                .forEach(blob -> {
                    String filePath = generateReferenceDir(referenceName) + fileUtils.getFilenameFromPath(blob.getName());
                    if (fileUtils.exists(filePath)) {
                        LOG.info(String.format("Reference %s already exists", blob.getName()));
                    } else {
                        fileUtils.mkdir(filePath);
                        gcsService.downloadBlobTo(blob, filePath);
                    }
                });
        return getReferencePathByName(referenceName);
    }

    private String generateReferenceDir(String referenceName) {
        return fileUtils.getCurrentPath() + allReferencesLocalDir + referenceName + "/";
    }

    private String getReferencePathByName(String referenceName) {
        return generateReferenceDir(referenceName) + referenceName + referenceFileExtension;
    }
}
