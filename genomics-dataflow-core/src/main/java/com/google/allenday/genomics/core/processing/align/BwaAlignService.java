package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.model.Instrument;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class BwaAlignService extends AlignService {

    public final static String BWA_DIR_NAME = "bwa-master";

    private final static String CMD_APT_UPDATE = "apt-get update";
    private final static String CMD_INSTALL_UNZIP = "apt-get install unzip -y";
    private final static String CMD_INSTALL_WGET = "apt-get install wget -y";
    private final static String CMD_DOWNLOAD_BWA = "wget https://github.com/lh3/bwa/archive/master.zip";
    private final static String CMD_UNZIP_BWA = "unzip master.zip";
    private final static String CMD_RM_BWA_ARCHIVE = "rm -f master.zip";
    private final static String CMD_INSTALL_MAKE = "apt-get install build-essential -y";
    private final static String CMD_INSTALL_ZLIB = "apt-get install zlib1g-dev -y";
    private final static String CMD_MAKE = String.format("cd %s; make", BWA_DIR_NAME);

    private final static String ALIGN_COMMAND_ILLUMINA_SINGLE = "./bwa-master/bwa mem %1$s %2$s > %3$s";
    private final static String ALIGN_COMMAND_ILLUMINA_SINGLE_SHORT =
            "./bwa-master/bwa aln %1$s %2$s > reads.sai; ./bwa-master/bwa samse %1$s reads.sai %2$s > %3$";


    private final static String ALIGN_COMMAND_ILLUMINA_PAIRED_1 =
            "./bwa-master/bwa aln %1$s %2$s > %4$s; ./bwa-master/bwa aln %1$s %3$s > %5$s";
    private final static String ALIGN_COMMAND_ILLUMINA_PAIRED_2 =
            "./bwa-master/bwa sampe %1$s %5$s %6$s %2$s %3$s > %4$s";
    private final static String TEMP_SAI_NAME_1 = "read1.sai";
    private final static String TEMP_SAI_NAME_2 = "read2.sai";


    private final static String ALIGN_COMMAND_OXFORD_NANOPORE =
            "./bwa-master/bwa mem -x ont2d %1$s %2$s > %3$s";
    private final static String ALIGN_COMMAND_PAC_BIO =
            "./bwa-master/bwa mem -x pacbio %1$s %2$s > %3$s";

    private final static String BWA_REF_INDEX =
            "./bwa-master/bwa index %s";

    private final static String[] REFERENCES_BWA_INDEX_EXTENSION = {".amb", ".ann", ".bwt", ".pac", ".sa"};

    private final static String DEFAULT_MINIMAP_INSTALATION_PATH = "/";


    public BwaAlignService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor, FileUtils fileUtils) {
        super(workerSetupService, cmdExecutor, fileUtils);
    }

    @Override
    public void setup() {
        workerSetupService.setupByCommands(new String[]{
                CMD_APT_UPDATE,
                CMD_INSTALL_WGET,
                CMD_INSTALL_UNZIP,
                CMD_DOWNLOAD_BWA,
                CMD_UNZIP_BWA,
                CMD_RM_BWA_ARCHIVE,
                CMD_INSTALL_MAKE,
                CMD_INSTALL_ZLIB,
                CMD_MAKE,
        });
    }

    @Override
    public String alignFastq(String referencePath, List<String> localFastqPaths, String workDir,
                             String outPrefix, String outSuffix, String readGroupName, String instrumentName) {

        checkForRefIndexAndCreateIfNeed(referencePath);

        Instrument instrument = null;
        try {
            instrument = Instrument.valueOf(instrumentName);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(String.format("Instrument %s is not supported", instrumentName));

        }
        String alignedSamName = outPrefix + "_" + outSuffix + SAM_FILE_PREFIX;
        String alignedSamPath = workDir + alignedSamName;

        List<String> bwaCommands = buildCommands(instrument, referencePath, localFastqPaths, alignedSamPath);

        for (String minimapCommand : bwaCommands) {
            Triplet<Boolean, Integer, String> result = cmdExecutor.executeCommand(minimapCommand);
            if (!result.getValue0()) {
                throw new AlignException(minimapCommand, result.getValue1());
            }
        }
        fileUtils.deleteFile(TEMP_SAI_NAME_1);
        fileUtils.deleteFile(TEMP_SAI_NAME_2);
        return alignedSamPath;
    }

    private void checkForRefIndexAndCreateIfNeed(String referencePath) {
        boolean hasMissedIndexFiles = Stream.of(REFERENCES_BWA_INDEX_EXTENSION)
                .map(refIndex -> fileUtils.exists(referencePath + refIndex)).anyMatch(exists -> !exists);
        if (hasMissedIndexFiles){
            String command = String.format(BWA_REF_INDEX, referencePath);
            Triplet<Boolean, Integer, String> result = cmdExecutor.executeCommand(command);
            if (!result.getValue0()) {
                throw new AlignException(command, result.getValue1());
            }
        }
    }

    private List<String> buildCommands(Instrument instrument, String referencePath, List<String> localFastqPaths, String alignedSamPath) {
        if (instrument == Instrument.ILLUMINA || instrument == Instrument.LS454 || instrument == Instrument.MGISEQ) {
            if (localFastqPaths.size() == 1) {
                return Collections.singletonList(
                        String.format(ALIGN_COMMAND_ILLUMINA_SINGLE, referencePath, localFastqPaths.get(0), alignedSamPath)
                );
            } else if (localFastqPaths.size() == 2) {
                List<String> alignCommands = new ArrayList<>();
                alignCommands.add(String.format(ALIGN_COMMAND_ILLUMINA_PAIRED_1, referencePath,
                        localFastqPaths.get(0), localFastqPaths.get(1), TEMP_SAI_NAME_1, TEMP_SAI_NAME_2));
                alignCommands.add(String.format(ALIGN_COMMAND_ILLUMINA_PAIRED_2, referencePath,
                        localFastqPaths.get(0), localFastqPaths.get(1), alignedSamPath, TEMP_SAI_NAME_1, TEMP_SAI_NAME_2));
                return alignCommands;
            } else {
                throw new AlignException("Wrong FASTQ files count");
            }
        } else if (instrument == Instrument.OXFORD_NANOPORE) {
            if (localFastqPaths.size() == 1) {
                return Collections.singletonList(
                        String.format(ALIGN_COMMAND_PAC_BIO, referencePath, localFastqPaths.get(0), alignedSamPath)
                );
            } else {
                throw new AlignException("Wrong FASTQ files count");
            }

        } else if (instrument == Instrument.PACBIO_SMRT) {
            if (localFastqPaths.size() == 1) {
                return Collections.singletonList(
                        String.format(ALIGN_COMMAND_OXFORD_NANOPORE, referencePath, localFastqPaths.get(0), alignedSamPath)
                );
            } else {
                throw new AlignException("Wrong FASTQ files count");
            }
        } else {
            throw new AlignException(String.format("Not supported instrument %s for %s", instrument.name(), this.getClass().getName()));
        }
    }
}
