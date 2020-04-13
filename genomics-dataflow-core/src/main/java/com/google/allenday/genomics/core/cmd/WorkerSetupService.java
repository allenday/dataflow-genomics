package com.google.allenday.genomics.core.cmd;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WorkerSetupService implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(WorkerSetupService.class);

    private CmdExecutor cmdExecutor;

    public WorkerSetupService(CmdExecutor cmdExecutor) {
        this.cmdExecutor = cmdExecutor;
    }

    public void setupByCommands(List<Pair<String, Boolean>> commands) {
        LOG.info("Start setup");
        commands.forEach(command -> cmdExecutor.executeCommand(command.getValue0(), command.getValue1()));
        LOG.info("Finish setup");
    }

    public void setupByCommands(String[] commands) {
        setupByCommands(Stream.of(commands).map(command -> Pair.with(command, true)).collect(Collectors.toList()));
    }

}
