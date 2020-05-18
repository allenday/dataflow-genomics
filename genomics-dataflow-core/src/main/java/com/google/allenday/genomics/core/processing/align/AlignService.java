package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.worker.cmd.CmdExecutor;
import com.google.allenday.genomics.core.worker.WorkerSetupService;
import com.google.allenday.genomics.core.utils.FileUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public abstract class AlignService implements Serializable {

    public final static String SAM_FILE_PREFIX = ".sam";

    protected WorkerSetupService workerSetupService;
    protected CmdExecutor cmdExecutor;
    protected FileUtils fileUtils;

    public AlignService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor,
                        FileUtils fileUtils) {
        this.workerSetupService = workerSetupService;
        this.cmdExecutor = cmdExecutor;
        this.fileUtils = fileUtils;
    }

    public abstract void setup();

    public abstract String alignFastq(String referencePath, List<String> localFastqPaths, String workDir,
                                      String outPrefix, String outSuffix, String readGroupName, String instrumentName) throws AlignException;

    public static class AlignException extends IOException {

        public AlignException(String command, int code) {
            super(String.format("Align command %s failed with code %d", command, code));
        }

        public AlignException(String text) {
            super(String.format("Align failed: %s", text));
        }
    }
}
