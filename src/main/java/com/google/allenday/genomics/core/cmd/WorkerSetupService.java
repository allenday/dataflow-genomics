package com.google.allenday.genomics.core.cmd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.stream.Stream;

public class WorkerSetupService implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(WorkerSetupService.class);

    private CmdExecutor cmdExecutor;

    public WorkerSetupService(CmdExecutor cmdExecutor) {
        this.cmdExecutor = cmdExecutor;
    }

    public void setupByCommands(String[] commands) {
        LOG.info("Start setup");
        Stream.of(commands)
                .forEach(command -> cmdExecutor.executeCommand(command));
        LOG.info("Finish setup");
    }

}
