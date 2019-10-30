package com.google.allenday.genomics.core.reference;

import com.google.allenday.genomics.core.gene.ReferenceDatabase;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.cloud.storage.BlobId;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ReferencesProvider implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(ReferencesProvider.class);

    private static final String DEFAULT_REFERENCE_FILE_EXTENSION = ".fa";
    private static final String DEFAULT_ALL_REFERENCE_LOCAL_DIR = "reference/";

    private FileUtils fileUtils;
    private String allReferencesDirGcsUri;
    private String allReferencesLocalDir;
    private String referenceFileExtension;

    public ReferencesProvider(FileUtils fileUtils, String allReferencesDirGcsUri) {
        this(fileUtils, allReferencesDirGcsUri, DEFAULT_REFERENCE_FILE_EXTENSION, DEFAULT_ALL_REFERENCE_LOCAL_DIR);
    }

    public ReferencesProvider(FileUtils fileUtils, String allReferencesDirGcsUri, String referenceFileExtension) {
        this(fileUtils, allReferencesDirGcsUri, referenceFileExtension, DEFAULT_ALL_REFERENCE_LOCAL_DIR);
    }

    public ReferencesProvider(FileUtils fileUtils, String allReferencesDirGcsUri, String referenceFileExtension, String allReferencesLocalDir) {
        this.fileUtils = fileUtils;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
        this.referenceFileExtension = referenceFileExtension;
        this.allReferencesLocalDir = allReferencesLocalDir;
    }

    public Pair<ReferenceDatabase, String> findReference(GCSService gcsService, String referenceName) {
        BlobId blobIdFromUri = gcsService.getBlobIdFromUri(allReferencesDirGcsUri);
        List<String> dbFilesUris = new ArrayList<>();
        gcsService.getAllBlobsIn(blobIdFromUri.getBucket(), blobIdFromUri.getName())
                .stream()
                .filter(blob -> blob.getName().contains(referenceName))
                .forEach(blob -> {
                    dbFilesUris.add(gcsService.getUriFromBlob(blob.getBlobId()));
                    String filePath = generateReferenceDir(referenceName) + fileUtils.getFilenameFromPath(blob.getName());
                    if (fileUtils.exists(filePath)) {
                        LOG.info(String.format("Reference %s already exists", blob.getName()));
                    } else {
                        fileUtils.mkdirFromUri(filePath);
                        gcsService.downloadBlobTo(blob, filePath);
                    }
                });
        String fastaLocalPath = getReferencePathByName(referenceName);
        return Pair.with(new ReferenceDatabase(referenceName, dbFilesUris), fastaLocalPath);
    }

    private String generateReferenceDir(String referenceName) {
        return fileUtils.getCurrentPath() + allReferencesLocalDir + referenceName + "/";
    }

    private String getReferencePathByName(String referenceName) {
        return generateReferenceDir(referenceName) + referenceName + referenceFileExtension;
    }
}
