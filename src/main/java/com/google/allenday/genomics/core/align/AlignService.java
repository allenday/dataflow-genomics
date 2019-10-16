package com.google.allenday.genomics.core.align;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.io.FileUtils;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.List;

public class AlignService implements Serializable {

    private final static String MINIMAP_VERSION = "2.17";
    public final static String MINIMAP_NAME = String.format("minimap2-%s_x64-linux", MINIMAP_VERSION);
    private final static String MINIMAP_ARCHIVE_FILE_NAME = String.format("%s.tar.bz2", MINIMAP_NAME);

    private final static String CMD_APT_UPDATE = "apt-get update";
    private final static String CMD_INSTALL_WGET = "apt-get install wget -y";
    private final static String CMD_INSTALL_BZIP2 = "apt-get install bzip2 -y";
    private final static String CMD_DOWNLOAD_MONIMAP =
            String.format("wget https://github.com/lh3/minimap2/releases/download/v%s/%s",
                    MINIMAP_VERSION,
                    MINIMAP_ARCHIVE_FILE_NAME);
    private final static String CMD_UNTAR_MINIMAP_PATTERN = "tar -xvjf %s -C %s";
    private final static String CMD_RM_MINIMAP_ARCHIVE = String.format("rm -f %s", MINIMAP_ARCHIVE_FILE_NAME);

    private final static String SAM_FILE_PREFIX = ".sam";

    private final static String ALIGN_COMMAND_PATTERN = "./%s/minimap2" +
            " -ax sr %s %s" +
            " -R '@RG\tID:minimap2\tPL:ILLUMINA\tPU:NONE\tSM:RSP11055' " +
            "> %s";

    private final static String DEFAULT_MINIMAP_INSTALATION_PATH = "/";
    private WorkerSetupService workerSetupService;
    private CmdExecutor cmdExecutor;
    private FileUtils fileUtils;

    public AlignService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor, FileUtils fileUtils) {
        this.workerSetupService = workerSetupService;
        this.cmdExecutor = cmdExecutor;
        this.fileUtils = fileUtils;
    }

    public void setupMinimap2() {
        workerSetupService.setupByCommands(new String[]{
                CMD_APT_UPDATE,
                CMD_INSTALL_WGET,
                CMD_INSTALL_BZIP2,
                CMD_DOWNLOAD_MONIMAP,
                String.format(CMD_UNTAR_MINIMAP_PATTERN, MINIMAP_ARCHIVE_FILE_NAME, fileUtils.getCurrentPath()),
                CMD_RM_MINIMAP_ARCHIVE
        });
    }


    public String alignFastq(String referencePath, List<String> localFastqPaths, String workDir,
                             String outPrefix, String outSuffix) {
        String alignedSamName = outPrefix + "_" + outSuffix + SAM_FILE_PREFIX;
        String alignedSamPath = workDir + alignedSamName;

        String joinedSrcFiles = String.join(" ", localFastqPaths);
        String minimapCommand = String.format(ALIGN_COMMAND_PATTERN, MINIMAP_NAME, referencePath,
                joinedSrcFiles, alignedSamPath);

        Pair<Boolean, Integer> result = cmdExecutor.executeCommand(minimapCommand);
        if (!result.getValue0()) {
            throw new AlignException(minimapCommand, result.getValue1());
        }
        return alignedSamPath;
    }

    public static class AlignException extends RuntimeException {

        public AlignException(String command, int code) {
            super(String.format("Align command %s failed with code %d", command, code));
        }
    }
}
