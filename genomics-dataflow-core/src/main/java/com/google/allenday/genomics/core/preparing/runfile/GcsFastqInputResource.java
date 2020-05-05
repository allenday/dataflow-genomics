package com.google.allenday.genomics.core.preparing.runfile;

import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.cloud.storage.BlobId;

import java.io.IOException;
import java.io.InputStream;

public class GcsFastqInputResource extends FastqInputResource {

    private BlobId blobId;

    public GcsFastqInputResource(BlobId blobId) {
        this.blobId = blobId;
    }

    @Override
    public InputStream getInputStream(FileUtils fileUtils, GcsService gcsService) throws IOException {
        return fileUtils.getInputStreamFromReadChannel(getName(), gcsService.getBlobReadChannel(blobId));
    }

    @Override
    public String getName() {
        return blobId.getName();
    }
}
