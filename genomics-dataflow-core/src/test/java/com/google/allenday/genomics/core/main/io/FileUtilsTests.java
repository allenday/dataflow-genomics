package com.google.allenday.genomics.core.main.io;

import com.google.allenday.genomics.core.utils.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileUtilsTests {

    private final static String TEMP_DIR = "/tmp/genomics_dataflow_tests/";

    @Test
    public void testSaveDataToFile() throws IOException {
        FileUtils fileUtils = new FileUtils();

        byte[] data = "data".getBytes();
        File file = File.createTempFile("temp", null);

        fileUtils.saveDataToFile(data, file.getAbsolutePath());

        Assert.assertEquals(new String(data), new String(Files.readAllBytes(Paths.get(file.getAbsolutePath()))));
        file.deleteOnExit();
    }

    @Test
    public void makeDirTest() throws IOException {
        FileUtils fileUtils = new FileUtils();

        String dir1 = "temp1";
        String dir2 = "temp2";
        String dir3 = "temp3";

        String path1 = TEMP_DIR + String.format("%s", dir1);
        String path2 = TEMP_DIR + String.format("%s/", dir2);
        String path3 = TEMP_DIR + String.format("%s/temp4", dir3);

        fileUtils.mkdirFromUri(path1);
        fileUtils.mkdirFromUri(path2);
        fileUtils.mkdirFromUri(path3);

        Assert.assertTrue(!Files.exists(Paths.get(TEMP_DIR + dir1)));
        Assert.assertTrue(Files.exists(Paths.get(TEMP_DIR + dir2)));
        Assert.assertTrue(Files.exists(Paths.get(TEMP_DIR + dir3)));
    }

    @Test
    public void testGetFilenameFromPath() throws IOException {
        FileUtils fileUtils = new FileUtils();
        String filename = "filename";
        String path1 = String.format("/tmp/%s", filename);
        String path2 = filename;

        Assert.assertEquals(filename, fileUtils.getFilenameFromPath(path1));
        Assert.assertEquals(filename, fileUtils.getFilenameFromPath(path2));
    }

    @Test(expected = RuntimeException.class)
    public void testGetFilenameFromPathWithException() {
        FileUtils fileUtils = new FileUtils();
        String path3 = "/tmp/";
        fileUtils.getFilenameFromPath(path3);
    }

    @After
    public void finalizeTests() {
        new FileUtils().deleteDir(TEMP_DIR);
    }
}
