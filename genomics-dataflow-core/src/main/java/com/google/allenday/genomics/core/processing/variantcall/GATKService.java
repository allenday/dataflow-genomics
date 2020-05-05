package com.google.allenday.genomics.core.processing.variantcall;

import com.google.allenday.genomics.core.gcp.ResourceProvider;
import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import com.google.allenday.genomics.core.worker.WorkerSetupService;
import com.google.allenday.genomics.core.worker.cmd.CmdExecutor;
import com.google.allenday.genomics.core.worker.cmd.Commands;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GATKService extends VariantCallingService {
    private Logger LOG = LoggerFactory.getLogger(GATKService.class);

    private final static String GATK_VERSION = "4.1.6.0";

    private final static String GATK_NAME = String.format("gatk-%s", GATK_VERSION);
    private final static String GATK_ZIP_NAME = String.format("%s.zip", GATK_NAME);
    private final static String GATK_URI = String.format("https://github.com/broadinstitute/gatk/releases/download/%1$s/%2$s", GATK_VERSION, GATK_ZIP_NAME);


    private final static String DISABLE_GENERATION_OF_VCF_INDEX_PARAMETER = "--create-output-variant-index=False";

    private final static String HAPLOTYPE_CALLER_COMMAND_PATTERN = "./%1$s/gatk HaplotypeCaller -R %2$s -I %3$s -L %4$s -O %5$s";


    private WorkerSetupService workerSetupService;
    private CmdExecutor cmdExecutor;
    private boolean generateVcfIndex = false;

    public GATKService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor) {
        this.workerSetupService = workerSetupService;
        this.cmdExecutor = cmdExecutor;
    }

    @Override
    public void setup() {
        workerSetupService.setupByCommands(new String[]{
                Commands.CMD_APT_UPDATE,
                Commands.CMD_INSTALL_UNZIP,
                Commands.CMD_INSTALL_WGET,
                Commands.wget(GATK_URI),
                Commands.unzip(GATK_ZIP_NAME),
                Commands.CMD_INSTALL_PYTHON_2_7
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
        if (!generateVcfIndex) {
            command = command + " " + DISABLE_GENERATION_OF_VCF_INDEX_PARAMETER;
        }
        Triplet<Boolean, Integer, String> result = cmdExecutor.executeCommand(command);

        return Triplet.with(outFileUri, result.getValue0(), result.getValue2());
    }

    public void setGenerateVcfIndex(boolean generateVcfIndex) {
        this.generateVcfIndex = generateVcfIndex;
    }
}
