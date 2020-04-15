package com.google.allenday.genomics.core.cmd;

import com.google.allenday.genomics.core.utils.TimeUtils;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Random;

public class CmdExecutor implements Serializable {

    private final static int DEFAULT_RETRY_COUNT = 3;
    private int retryCount;

    public CmdExecutor() {
        this(DEFAULT_RETRY_COUNT);
    }

    public CmdExecutor(int retryCount) {
        this.retryCount = retryCount;
    }

    private Logger LOG = LoggerFactory.getLogger(CmdExecutor.class);


    public Triplet<Boolean, Integer, String> executeCommand(String cmdCommand, boolean inheritIO, boolean throwError) {
        return executeCommand(cmdCommand, inheritIO, retryCount, throwError);
    }

    public Triplet<Boolean, Integer, String> executeCommand(String cmdCommand, boolean throwError) {
        return executeCommand(cmdCommand, true, retryCount, throwError);
    }

    public Triplet<Boolean, Integer, String> executeCommand(String cmdCommand) {
        return executeCommand(cmdCommand, true, retryCount, true);
    }

    private Triplet<Boolean, Integer, String> executeCommand(String cmdCommand, boolean inheritIO, int retryCount, boolean throwError) {
        long startTime = System.currentTimeMillis();

        LOG.info(String.format("Executing command: %s", cmdCommand));
        ProcessBuilder processBuilder = new ProcessBuilder();
        if (inheritIO) {
            processBuilder.inheritIO();
        }
        processBuilder.command("bash", "-c", cmdCommand);

        StringBuilder response = new StringBuilder();
        try {

            Process process = processBuilder.start();
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                LOG.info(line);
                response.append(line);
            }

            int exitCode = process.waitFor();
            LOG.info(String.format("Execution finished in %s. Exited with error code: %d. Command: %s",
                    TimeUtils.formatDeltaTime(System.currentTimeMillis() - startTime), exitCode, cmdCommand));

            boolean success = exitCode == 0;
            if (!success) {
                if (retryCount > 0) {
                    int sleepMs = (int) (new Random().nextFloat() * 10000);
                    LOG.info(String.format("Sleep for %d ms", sleepMs));
                    Thread.sleep(sleepMs);
                    int retryLeft = retryCount - 1;
                    LOG.info(String.format("Retrying command: %s. Retry left: %d", cmdCommand, retryLeft));
                    return executeCommand(cmdCommand, inheritIO, retryLeft, throwError);
                } else if (throwError) {
                    throw new RuntimeException(String.format("Exited with error code: %d. Command: %s", exitCode, cmdCommand));
                }
            }
            return Triplet.with(success, exitCode, response.toString());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return Triplet.with(false, -1, response.toString());
        }
    }

}
