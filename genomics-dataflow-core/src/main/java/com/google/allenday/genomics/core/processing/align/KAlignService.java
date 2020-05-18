package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.worker.WorkerSetupService;
import com.google.allenday.genomics.core.worker.cmd.CmdExecutor;
import com.google.allenday.genomics.core.worker.cmd.Commands;
import org.javatuples.Triplet;

import java.io.IOException;
import java.io.Serializable;

public class KAlignService implements Serializable {
    private final static String KALIGN_COMMAND_PATTERN = "kalign < %s > %s";

    private WorkerSetupService workerSetupService;
    private CmdExecutor cmdExecutor;

    public KAlignService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor) {
        this.workerSetupService = workerSetupService;
        this.cmdExecutor = cmdExecutor;
    }

    public void setupKAlign2() {
        workerSetupService.setupByCommands(new String[]{
                Commands.CMD_APT_UPDATE,
                String.format(Commands.CMD_APT_GET_INSTALL_FORMAT, "kalign")
        });
    }

    public String kAlignFasta(String localFastaFilePath, String workDir,
                              String outPrefix, String outSuffix) throws IOException {
        String kAlignedSamName = outPrefix + "_" + outSuffix + AlignService.SAM_FILE_PREFIX;
        String kAlignedSamPath = workDir + kAlignedSamName;

        String kAlignCommand = String.format(KALIGN_COMMAND_PATTERN, localFastaFilePath, kAlignedSamPath);

        Triplet<Boolean, Integer, String> result = cmdExecutor.executeCommand(kAlignCommand);
        if (!result.getValue0()) {
            throw new KAlignException(kAlignCommand, result.getValue1());
        }
        return kAlignedSamPath;
    }

    public static class KAlignException extends IOException {

        public KAlignException(String command, int code) {
            super(String.format("KAlign command %s failed with code %d", command, code));
        }
    }
}
