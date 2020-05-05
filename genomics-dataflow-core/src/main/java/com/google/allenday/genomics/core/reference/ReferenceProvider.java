package com.google.allenday.genomics.core.reference;

import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.cloud.storage.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

public class ReferenceProvider implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(ReferenceProvider.class);

    private static final String DEFAULT_ALL_REFERENCE_LOCAL_DIR = "reference/";

    private FileUtils fileUtils;
    private String allReferencesLocalDir;

    public ReferenceProvider(FileUtils fileUtils) {
        this(fileUtils, DEFAULT_ALL_REFERENCE_LOCAL_DIR);
    }

    public ReferenceProvider(FileUtils fileUtils, String allReferencesLocalDir) {
        this.fileUtils = fileUtils;
        this.allReferencesLocalDir = allReferencesLocalDir;
    }

    public ReferenceDatabase getReferenceDbWithDownload(GcsService gcsService,
                                                        ReferenceDatabaseSource dbSource) throws IOException {
        return getDbFilesUris(gcsService, dbSource, true);
    }

    public ReferenceDatabase getReferenceDd(GcsService gcsService,
                                            ReferenceDatabaseSource dbSource) throws IOException {
        return getDbFilesUris(gcsService, dbSource, false);
    }

    private ReferenceDatabase getDbFilesUris(GcsService gcsService, ReferenceDatabaseSource dbSource,
                                             boolean withDownload) throws IOException {

        Optional<Blob> fastaBlob = dbSource.getReferenceBlob(gcsService, fileUtils);
        Optional<Blob> indexBlob = dbSource.getReferenceIndexBlob(gcsService, fileUtils);

        if (!fastaBlob.isPresent()) {
            throw new IOException(String.format("Fasta for %s does not exists!", dbSource.getName()));
        }

        String fastaGcsUri = gcsService.getUriFromBlob(fastaBlob.get().getBlobId());
        ReferenceDatabase.Builder referenceDatabaseBuilder = new ReferenceDatabase.Builder(dbSource.getName(),
                fastaGcsUri);
        if (indexBlob.isPresent()) {
            referenceDatabaseBuilder = referenceDatabaseBuilder.withIndexUri(gcsService.getUriFromBlob(indexBlob.get().getBlobId()));
        }

        if (withDownload) {
            String filePath = generateReferenceDir(dbSource.getName()) +
                    fileUtils.getFilenameFromPath(fastaGcsUri);
            if (fileUtils.exists(filePath)) {
                LOG.info(String.format("Reference %s already exists", dbSource.getName()));
            } else {
                fileUtils.mkdirFromUri(filePath);
                gcsService.downloadBlobTo(gcsService.getBlob(gcsService.getBlobIdFromUri(fastaGcsUri)), filePath);
            }
            referenceDatabaseBuilder.withfastaLocalPath(filePath);
        }
        return referenceDatabaseBuilder.build();
    }

    private String generateReferenceDir(String referenceName) {
        return fileUtils.getCurrentPath() + allReferencesLocalDir + referenceName + "/";
    }
}