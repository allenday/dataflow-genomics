package com.google.allenday.genomics.core.preparing.sra;

import com.google.allenday.genomics.core.worker.cmd.CmdExecutor;
import com.google.allenday.genomics.core.worker.cmd.Commands;
import com.google.allenday.genomics.core.worker.WorkerSetupService;
import com.google.allenday.genomics.core.utils.FileUtils;
import org.javatuples.Triplet;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class SraToolsService implements Serializable {

    public final static String SRA_TOOLS_VERSION = "2.10.0";
    public final static String SRA_TOOLS_SETUP_SCRIPT_FILE = "setup-apt.sh";
    public final static String SRA_TOOLS_SETUP_SCRIPT_URL =
            String.format("https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/%1$s/%2$s",
                    SRA_TOOLS_VERSION,
                    SRA_TOOLS_SETUP_SCRIPT_FILE);

    private final static String CMD_FASTQ_DUMP_WITH_SPLIT_AND_SKIP_TECHNICAL =
            "/usr/local/ncbi/sra-tools/bin/fastq-dump %1$s --split-files --skip-technical -O %2$s";

    private WorkerSetupService workerSetupService;
    private CmdExecutor cmdExecutor;
    private FileUtils fileUtils;

    public SraToolsService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor, FileUtils fileUtils) {
        this.workerSetupService = workerSetupService;
        this.cmdExecutor = cmdExecutor;
        this.fileUtils = fileUtils;
    }

    public void setup() {
        workerSetupService.setupByCommands(new String[]{
                Commands.CMD_APT_UPDATE,
                Commands.CMD_INSTALL_UNZIP,
                Commands.CMD_INSTALL_CURL,
                Commands.CMD_INSTALL_WGET,
                Commands.CMD_INSTALL_ZLIB1G_DEV,
                Commands.CMD_INSTALL_LIB_XML2_DEV,
                Commands.wget(SRA_TOOLS_SETUP_SCRIPT_URL),
                Commands.sh(SRA_TOOLS_SETUP_SCRIPT_FILE)
        });
    }

    public List<String> retrieveSraFromFastq(String accession, String outputDir) throws IOException {
        Triplet<Boolean, Integer, String> results =
                cmdExecutor.executeCommand(String.format(CMD_FASTQ_DUMP_WITH_SPLIT_AND_SKIP_TECHNICAL, accession, outputDir));
        if (results.getValue0()) {
            return fileUtils.listOfFilesInDir(outputDir);
        } else {
            throw new IOException(String.format("Sra Tools failed with %d (%s)", results.getValue1(), results.getValue2()));
        }
    }
}
