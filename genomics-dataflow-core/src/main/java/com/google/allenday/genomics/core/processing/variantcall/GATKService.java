package com.google.allenday.genomics.core.processing.variantcall;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import com.google.allenday.genomics.core.utils.ResourceProvider;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GATKService extends VariantCallingService {
    private Logger LOG = LoggerFactory.getLogger(GATKService.class);

    private final static String GATK_VERSION = "4.1.6.0";

    private final static String GATK_NAME = String.format("gatk-%s", GATK_VERSION);
    private final static String GATK_ZIP_NAME = String.format("%s.zip", GATK_NAME);
    private final static String GATK_URI = String.format("https://github.com/broadinstitute/gatk/releases/download/%1$s/%2$s", GATK_VERSION, GATK_ZIP_NAME);

    private final static String CMD_APT_UPDATE = "apt-get update";
    private final static String CMD_INSTALL_UNZIP = "apt-get install unzip -y";
    private final static String CMD_INSTALL_WGET = "apt-get install wget -y";
    private final static String CMD_INSTALL_PYTHON_2_7 = "apt-get install python -y";
    private final static String CMD_DOWNLOAD_GATK = String.format("wget %s", GATK_URI);
    private final static String CMD_UNZIP_GATK = String.format("unzip %s", GATK_ZIP_NAME);

    private final static String HAPLOTYPE_CALLER_COMMAND_PATTERN = "./%1$s/gatk HaplotypeCaller -R %2$s -I %3$s -L %4$s -O %5$s";

    private WorkerSetupService workerSetupService;
    private CmdExecutor cmdExecutor;

    public GATKService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor) {
        this.workerSetupService = workerSetupService;
        this.cmdExecutor = cmdExecutor;
    }

    @Override
    public void setup() {
        workerSetupService.setupByCommands(new String[]{
                CMD_APT_UPDATE,
                CMD_INSTALL_UNZIP,
                CMD_INSTALL_WGET,
                CMD_DOWNLOAD_GATK,
                CMD_UNZIP_GATK,
                CMD_INSTALL_PYTHON_2_7
        });
    }

    @Override
    public Triplet<String, Boolean, String> processSampleWithVariantCaller(ResourceProvider resourceProvider,
                                                                           String outDirGcsUri, String outFileNameWithoutExt,
                                                                           String bamUri, String baiUri, String region,
                                                                           ReferenceDatabase referenceDatabase, String readGroupName) {
        String outFileUri = outDirGcsUri + outFileNameWithoutExt + DEEP_VARIANT_RESULT_EXTENSION;
        String command = String.format(HAPLOTYPE_CALLER_COMMAND_PATTERN,
                GATK_NAME, referenceDatabase.getFastaGcsUri(), bamUri, region, outFileUri);
        Triplet<Boolean, Integer, String> result = cmdExecutor.executeCommand(command);

        return Triplet.with(outFileUri, result.getValue0(), result.getValue2());
    }
}
