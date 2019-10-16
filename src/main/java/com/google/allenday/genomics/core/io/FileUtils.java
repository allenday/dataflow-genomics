package com.google.allenday.genomics.core.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils implements Serializable {

    private static Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    public void saveDataToFile(byte[] data, String filename) throws IOException {
        File destFile = new File(filename);
        destFile.createNewFile();
        OutputStream os = new FileOutputStream(new File(filename));
        os.write(data);
        os.close();
    }

    public String getCurrentPath(){
        Path currentRelativePath = Paths.get("");
        return currentRelativePath.toAbsolutePath().toString() + "/";
    }

    public String makeUniqueDirWithTimestampAndSuffix(String suffix) throws RuntimeException{
        String workingDir = getCurrentPath() + System.currentTimeMillis() + "_" + suffix + "/";
        mkdir(workingDir);
        return workingDir;
    }

    public void mkdir(String path) throws RuntimeException{
        Path dir;
        if (path.charAt(path.length() - 1) != '/') {
            dir = Paths.get(path).getParent();
        } else {
            dir = Paths.get(path);
        }
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            LOG.info(String.format("Dir %s created", dir.toString()));
        }
    }

    public byte[] readFileToByteArray(String filePath) throws IOException {
        return Files.readAllBytes(new File(filePath).toPath());
    }

    public long getFileSizeMegaBytes(String filePath) {
        return new File(filePath).length() / (1024 * 1024);
    }

    public String getFilenameFromPath(String filePath) {
        if (filePath.contains("/")) {
            return filePath.split("/")[filePath.split("/").length - 1];
        } else {
            return filePath;
        }
    }

    public String changeFileExtension(String fileName, String newFileExtension) {
        if (fileName.contains(".")) {
            return fileName.split("\\.")[0] + newFileExtension;
        } else {
            return fileName + newFileExtension;
        }
    }


    public void deleteFile(String filePath) {
        File fileToDelete = new File(filePath);
        if (fileToDelete.exists()) {
            boolean delete = fileToDelete.delete();
            if (delete) {
                LOG.info(String.format("File %s deleted", filePath));
            }
        }
    }

    public void deleteDir(String dirPath) {
        try {
            org.apache.commons.io.FileUtils.deleteDirectory(new File(dirPath));
            LOG.info(String.format("Dir %s deleted", dirPath));
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    public long getFreeDiskSpace() {
        return new File("/").getFreeSpace();
    }

    public boolean exists(String filePath) {
        return Files.exists(Paths.get(filePath));
    }

    public boolean contentEquals(File file1, File file2) throws IOException{
        return org.apache.commons.io.FileUtils.contentEquals(file1, file2);
    }
}
