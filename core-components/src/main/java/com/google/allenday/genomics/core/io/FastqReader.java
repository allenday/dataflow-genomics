package com.google.allenday.genomics.core.io;

import com.google.cloud.ReadChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FastqReader implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(FastqReader.class);

    private final static int BUFFER_SIZE = 16 * 1024;
    private final static String NEW_LINE_INDICATION = "\n";
    private final static int FASTQ_READ_LINE_COUNT = 4;

    private String removeEmptyLines(String src) {
        String withoutEmptyLines = String.join(NEW_LINE_INDICATION, Stream.of(src.trim().split(NEW_LINE_INDICATION))
                .map(String::trim).filter(str -> !str.isEmpty())
                .toArray(String[]::new));
        if (src.startsWith("\n")) {
            withoutEmptyLines = NEW_LINE_INDICATION + withoutEmptyLines;
        }
        if (src.endsWith("\n")) {
            withoutEmptyLines = withoutEmptyLines + NEW_LINE_INDICATION;
        }
        return withoutEmptyLines;
    }

    private long sizeOfStringList(List<String> list) {
        return list.stream().mapToInt(str -> str.getBytes().length).sum();
    }

    /**
     * Reads FastQ data line by line from GCS. Helps to eliminate Out Of Memory problems with large FastQ files
     */
    public void readFastqBlobWithSizeLimit(ReadChannel readChannel, long batchSize, Callback callback) throws IOException {
        if (batchSize < BUFFER_SIZE) {
            batchSize = BUFFER_SIZE;
        }

        ByteBuffer bytes = ByteBuffer.allocate(BUFFER_SIZE);
        String fastqTail = "";
        List<String> linesCollector = new ArrayList<>();

        int indexCounter = 0;
        long currentReadSize = 0;
        long timeCount = 0;
        while (readChannel.read(bytes) > 0) {
            bytes.flip();
            String newLines = removeEmptyLines(StandardCharsets.UTF_8.decode(bytes).toString());
            currentReadSize += newLines.getBytes().length;
            String readString = fastqTail + newLines;
            bytes.clear();

            long start = System.currentTimeMillis();
            List<String> lines = Arrays.asList(readString.split(NEW_LINE_INDICATION));

            int startOfLastFastq = ((lines.size() - 1) / FASTQ_READ_LINE_COUNT) * FASTQ_READ_LINE_COUNT;

            List<String> currentLines = lines.subList(0, startOfLastFastq);
            List<String> tailLines = lines.subList(startOfLastFastq, lines.size());

            fastqTail = String.join(NEW_LINE_INDICATION, tailLines);
            if (readString.endsWith(NEW_LINE_INDICATION)) {
                fastqTail += NEW_LINE_INDICATION;
            }

            currentLines = currentLines.stream()
                    .filter(line -> !line.isEmpty())
                    .map(line -> line + NEW_LINE_INDICATION)
                    .collect(Collectors.toList());
            linesCollector.addAll(currentLines);

            if (currentReadSize + BUFFER_SIZE > batchSize) {
                String output = String.join("", linesCollector);
                callback.onFindFastqPart(output, indexCounter);
                indexCounter++;

                linesCollector.clear();
                currentReadSize = fastqTail.getBytes().length;
            }
            timeCount += System.currentTimeMillis() - start;
        }
        if (linesCollector.size() > 0) {
            callback.onFindFastqPart(String.join("", linesCollector) + fastqTail, indexCounter);
        }
        LOG.info(String.format("Spent time ms: %d", timeCount));
    }

    /**
     * Reads FastQ data line by line from GCS. Helps to eliminate Out Of Memory problems with large FastQ files
     */
    public void readFastqBlobWithReadCountLimit(ReadChannel readChannel, int batchSize, Callback callback) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(BUFFER_SIZE);
        String fastqTail = "";
        List<String> linesCollector = new ArrayList<>();

        int indexCounter = 0;
        long timeCount = 0;

        while (readChannel.read(bytes) > 0) {
            bytes.flip();
            String newLines = removeEmptyLines(StandardCharsets.UTF_8.decode(bytes).toString());
            String readString = fastqTail + newLines;
            bytes.clear();

            long start = System.currentTimeMillis();
            List<String> lines = Arrays.asList(readString.split(NEW_LINE_INDICATION));

            int startOfLastFastq = ((lines.size() - 1) / FASTQ_READ_LINE_COUNT) * FASTQ_READ_LINE_COUNT;

            List<String> currentLines = lines.subList(0, startOfLastFastq);
            List<String> tailLines = lines.subList(startOfLastFastq, lines.size());

            fastqTail = String.join(NEW_LINE_INDICATION, tailLines);
            if (readString.endsWith(NEW_LINE_INDICATION)) {
                fastqTail += NEW_LINE_INDICATION;
            }

            currentLines = currentLines.stream()
                    .filter(line -> !line.isEmpty())
                    .map(line -> line + NEW_LINE_INDICATION)
                    .collect(Collectors.toList());
            linesCollector.addAll(currentLines);

            while (linesCollector.size() > batchSize * FASTQ_READ_LINE_COUNT) {
                List<String> outputList = linesCollector.subList(0, batchSize * FASTQ_READ_LINE_COUNT);
                String output = String.join("", outputList);
                callback.onFindFastqPart(output, indexCounter);
                indexCounter++;

                linesCollector = linesCollector.subList(batchSize * FASTQ_READ_LINE_COUNT, linesCollector.size());
            }
            timeCount += System.currentTimeMillis() - start;
        }
        if (linesCollector.size() > 0) {
            callback.onFindFastqPart(String.join("", linesCollector) + fastqTail, indexCounter);
        }
        LOG.info(String.format("Spent time ms: %d", timeCount));
    }

    public static interface Callback {

        void onFindFastqPart(String fastqPart, int index);
    }
}
