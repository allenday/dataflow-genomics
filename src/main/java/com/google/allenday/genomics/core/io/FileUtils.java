package com.google.allenday.genomics.core.io;

import org.javatuples.Pair;
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

    public String getCurrentPath() {
        Path currentRelativePath = Paths.get("");
        String currentPath = currentRelativePath.toAbsolutePath().toString();
        if (currentPath.charAt(currentPath.length() - 1) != '/') {
            currentPath = currentPath + '/';
        }
        return currentPath;
    }

    public String makeDirByCurrentTimestampAndSuffix(String suffix) throws RuntimeException {
        String workingDir = getCurrentPath() + System.currentTimeMillis() + "_" + suffix + "/";
        mkdirFromUri(workingDir);
        return workingDir;
    }

    public void mkdirFromUri(String path) throws RuntimeException {
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

    public double getFileSizeMegaBytes(String filePath) {
        return new File(filePath).length() / (double) (mbToBytes(1));
    }

    public long mbToBytes(long mbValue) {
        return mbValue * (1024 * 1024);
    }

    public String getFilenameFromPath(String filePath) {
        if (filePath.charAt(filePath.length() - 1) == '/') {
            throw new NoFileNameException(filePath);
        }
        if (filePath.contains("/")) {
            return filePath.split("/")[filePath.split("/").length - 1];
        } else {
            return filePath;
        }
    }

    public String getDirFromPath(String filePath) {
        try {
            String filenameFromPath = getFilenameFromPath(filePath);
            return filePath.replace(filenameFromPath, "");
        } catch (NoFileNameException exc) {
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

    public Pair<String, String> splitFilenameToBaseAndExtension(String fileName) {
        if (fileName.contains(".")) {
            int firstDot = fileName.indexOf("\\.");
            return Pair.with(fileName.substring(0, firstDot), fileName.substring(firstDot));
        } else {
            return Pair.with(fileName, "");
        }
    }

    public Pair<String, String> splitFilenameAndExtension(String filenameWithExtension) {
        if (filenameWithExtension.contains(".")) {
            String name = filenameWithExtension.split("\\.")[0];
            return Pair.with(name, filenameWithExtension.replace(name, ""));
        } else {
            return Pair.with(filenameWithExtension, "");
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
            LOG.info(String.format("Free disk space: %d", getFreeDiskSpace()));
            org.apache.commons.io.FileUtils.deleteDirectory(new File(dirPath));
            LOG.info(String.format("Dir %s deleted", dirPath));
            LOG.info(String.format("Free disk space: %d", getFreeDiskSpace()));
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    public long getFreeDiskSpace(String path) {
        return new File(path).getFreeSpace();
    }

    public long getFreeDiskSpace() {
        return getFreeDiskSpace("/");
    }

    public boolean exists(String filePath) {
        return Files.exists(Paths.get(filePath));
    }

    public boolean contentEquals(File file1, File file2) throws IOException {
        return org.apache.commons.io.FileUtils.contentEquals(file1, file2);
    }

    public InputStream getInputStreamFromFile(String filePath) throws FileNotFoundException {
        return new FileInputStream(filePath);
    }

    private File createTempFileFromUri(String uri) throws IOException {
        String filenameFromPath = getFilenameFromPath(uri);
        return File.createTempFile(filenameFromPath, null);
    }

    public class NoFileNameException extends RuntimeException {
        public NoFileNameException(String path) {
            super(String.format("There is no file in path: %s", path));
        }
    }
}
