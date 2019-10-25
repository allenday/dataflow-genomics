package com.google.allenday.genomics.core.align;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.io.FileUtils;
import org.javatuples.Pair;

import java.io.Serializable;

public class KAlignService implements Serializable {
    public final static String FASTA_FILE_EXTENSION = ".fasta";

    private final static String CMD_APT_UPDATE = "apt-get update";
    private final static String KALIGN_COMMAND_PATTERN = "kalign < %s > %s";

    private WorkerSetupService workerSetupService;
    private CmdExecutor cmdExecutor;

    public KAlignService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor) {
        this.workerSetupService = workerSetupService;
        this.cmdExecutor = cmdExecutor;
    }

    public void setupKAlign2() {
        workerSetupService.setupByCommands(new String[]{
                CMD_APT_UPDATE,
                "apt-get install kalign -y"
        });
    }

    public String kAlignFasta(String localFastaFilePath, String workDir,
                             String outPrefix, String outSuffix) {
        String kAlignedSamName = outPrefix + "_" + outSuffix + FASTA_FILE_EXTENSION;
        String kAlignedSamPath = workDir + kAlignedSamName;

        String kAlignCommand = String.format(KALIGN_COMMAND_PATTERN, localFastaFilePath, kAlignedSamPath);

        Pair<Boolean, Integer> result = cmdExecutor.executeCommand(kAlignCommand);
        if (!result.getValue0()) {
            throw new KAlignException(kAlignCommand, result.getValue1());
        }
        return kAlignedSamPath;
    }

    public static class KAlignException extends RuntimeException {

        public KAlignException(String command, int code) {
            super(String.format("KAlign command %s failed with code %d", command, code));
        }
    }
}
