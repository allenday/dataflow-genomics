package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.worker.cmd.CmdExecutor;
import com.google.allenday.genomics.core.worker.cmd.Commands;
import com.google.allenday.genomics.core.worker.WorkerSetupService;
import com.google.allenday.genomics.core.utils.FileUtils;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.List;

public class Minimap2AlignService extends AlignService {

    private final static String MINIMAP_VERSION = "2.17";
    public final static String MINIMAP_NAME = String.format("minimap2-%s_x64-linux", MINIMAP_VERSION);
    private final static String MINIMAP_ARCHIVE_FILE_NAME = String.format("%s.tar.bz2", MINIMAP_NAME);
    private final static String MINIMAP_ARCHIVE_URL = String.format("https://github.com/lh3/minimap2/releases/download/v%s/%s",
            MINIMAP_VERSION,
            MINIMAP_ARCHIVE_FILE_NAME);

    private final static String ALIGN_COMMAND_PATTERN = "./%s/minimap2" +
            " -ax %s %s %s" +
            " -R '@RG\\tID:%s\\tSM:%s' " +
            "> %s";

    private final static String MINIMAP_SHORT_READ_FLAG = "sr";
    private final static String MINIMAP_OXFORD_NANOPORE_FLAG = "map-ont";
    private final static String MINIMAP_PAC_BIO_FLAG = "map-pb";

    private final static String DEFAULT_MINIMAP_INSTALATION_PATH = "/";


    public Minimap2AlignService(WorkerSetupService workerSetupService, CmdExecutor cmdExecutor, FileUtils fileUtils) {
        super(workerSetupService, cmdExecutor, fileUtils);
    }

    @Override
    public void setup() {
        ArrayList<Pair<String, Boolean>> commands = new ArrayList<Pair<String, Boolean>>() {
            {
                add(Pair.with(Commands.CMD_APT_UPDATE, false));
                add(Pair.with(Commands.CMD_INSTALL_WGET, false));
                add(Pair.with(Commands.CMD_INSTALL_BZIP2, false));
                add(Pair.with(Commands.wget(MINIMAP_ARCHIVE_URL), true));
                add(Pair.with(Commands.untar(MINIMAP_ARCHIVE_FILE_NAME, fileUtils.getCurrentPath()), true));
                add(Pair.with(Commands.rm(MINIMAP_ARCHIVE_FILE_NAME), true));
            }
        };
        workerSetupService.setupByCommands(commands);
    }


    @Override
    public String alignFastq(String referencePath, List<String> localFastqPaths, String workDir,
                             String outPrefix, String outSuffix, String readGroupName, String instrumentName) throws AlignException {
        Instrument instrument = null;
        try {
            instrument = Instrument.valueOf(instrumentName);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(String.format("Instrument %s is not supported", instrumentName));
        }
        String alignedSamName = outPrefix + "_" + outSuffix + SAM_FILE_PREFIX;
        String alignedSamPath = workDir + alignedSamName;

        String joinedSrcFiles = String.join(" ", localFastqPaths);
        String minimapCommand = String.format(ALIGN_COMMAND_PATTERN, MINIMAP_NAME, getInstrumentMinimapFlag(instrument), referencePath,
                joinedSrcFiles, readGroupName, readGroupName, alignedSamPath);

        Triplet<Boolean, Integer, String> result = cmdExecutor.executeCommand(minimapCommand);
        if (!result.getValue0()) {
            throw new AlignException(minimapCommand, result.getValue1());
        }
        return alignedSamPath;
    }

    private String getInstrumentMinimapFlag(Instrument instrument) throws AlignException {
        if (instrument == Instrument.ILLUMINA || instrument == Instrument.LS454 || instrument == Instrument.MGISEQ) {
            return MINIMAP_SHORT_READ_FLAG;
        } else if (instrument == Instrument.OXFORD_NANOPORE) {
            return MINIMAP_OXFORD_NANOPORE_FLAG;
        } else if (instrument == Instrument.PACBIO_SMRT) {
            return MINIMAP_PAC_BIO_FLAG;
        } else {
            throw new AlignService.AlignException(String.format("Not supported instrument: %s", instrument.name()));
        }
    }


}
