package com.google.allenday.genomics.core.preparing.runfile;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.utils.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
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
    public boolean exists(GcsService gcsService) {
        try {
            HttpURLConnection.setFollowRedirects(false);
            HttpURLConnection con =
                    (HttpURLConnection) new URL(url).openConnection();
            con.setRequestMethod("HEAD");
            return (con.getResponseCode() == HttpURLConnection.HTTP_OK);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public String getName() {
        return url;
    }
}
