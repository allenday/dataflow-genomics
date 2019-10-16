package com.google.allenday.genomics.core.cmd;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;

public class CmdExecutor implements Serializable {

    private Logger LOG = LoggerFactory.getLogger(CmdExecutor.class);

    public Pair<Boolean, Integer> executeCommand(String cmdCommand) {
        LOG.info(String.format("Executing command: %s", cmdCommand));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", cmdCommand);

        try {

            Process process = processBuilder.start();

            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                LOG.info(line);
            }

            int exitCode = process.waitFor();
            LOG.info("\nExited with error code : " + exitCode);
            return Pair.with(exitCode == 0, exitCode);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return Pair.with(false, -1);
        }
    }
}
