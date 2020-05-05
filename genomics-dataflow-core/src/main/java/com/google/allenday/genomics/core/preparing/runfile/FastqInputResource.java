package com.google.allenday.genomics.core.preparing.runfile;

import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.gcp.GcsService;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public abstract class FastqInputResource implements Serializable {

    public abstract InputStream getInputStream(FileUtils fileUtils, GcsService gcsService) throws IOException;

    public abstract String getName();

}
