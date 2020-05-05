package com.google.allenday.genomics.core.preparing.runfile;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.utils.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class UrlFastqInputResource extends FastqInputResource {

    private String url;

    public UrlFastqInputResource(String url) {
        this.url = url;
    }

    @Override
    public InputStream getInputStream(FileUtils fileUtils, GcsService gcsService) throws IOException {
        return new URL(url).openStream();
    }

    @Override
    public String getName() {
        return url;
    }
}
