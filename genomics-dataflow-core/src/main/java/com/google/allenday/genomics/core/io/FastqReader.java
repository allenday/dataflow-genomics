package com.google.allenday.genomics.core.io;

import com.google.allenday.genomics.core.utils.TimeUtils;
import com.google.cloud.ReadChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FastqReader implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(FastqReader.class);

    private final static int BUFFER_SIZE = 64 * 1024;
    private final static String NEW_LINE_INDICATION = "\n";
    private final static int FASTQ_READ_LINE_COUNT = 4;

    private StringBuilder removeEmptyLines(String src) {
        List<String> fileteredList = Stream.of(src.trim().split(NEW_LINE_INDICATION))
                .map(String::trim).filter(str -> !str.isEmpty())
                .collect(Collectors.toList());

        StringBuilder withoutEmptyLines = new StringBuilder().append(String.join(NEW_LINE_INDICATION, fileteredList));
        fileteredList.clear();

        if (src.startsWith("\n")) {
            withoutEmptyLines.insert(0, NEW_LINE_INDICATION);
        }
        if (src.endsWith("\n")) {
            withoutEmptyLines.append(NEW_LINE_INDICATION);
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
            long start = System.currentTimeMillis();
            bytes.flip();
            StringBuilder newLines = removeEmptyLines(StandardCharsets.UTF_8.decode(bytes).toString());
            currentReadSize += newLines.toString().getBytes().length;
            StringBuilder readString = newLines.insert(0, fastqTail);
            bytes.clear();

            List<String> lines = Arrays.asList(readString.toString().split(NEW_LINE_INDICATION));

            int startOfLastFastq = ((lines.size() - 1) / FASTQ_READ_LINE_COUNT) * FASTQ_READ_LINE_COUNT;

            List<String> currentLines = new ArrayList<>(lines.subList(0, startOfLastFastq));
            List<String> tailLines = new ArrayList<>(lines.subList(startOfLastFastq, lines.size()));

            fastqTail = String.join(NEW_LINE_INDICATION, tailLines);
            if (readString.toString().endsWith(NEW_LINE_INDICATION)) {
                fastqTail += NEW_LINE_INDICATION;
            }
            readString.setLength(0);

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

    public void readFastqBlobWithReadCountLimit(ReadableByteChannel readChannel, int batchSize, Callback callback) throws IOException {
        readFastqBlobWithReadCountLimit(readChannel, batchSize, callback, false);
    }

    public void readFastqBlobWithReadCountLimit(ReadableByteChannel readChannel, int batchSize, Callback callback, boolean fixLines) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(BUFFER_SIZE);
        StringBuilder fastqDataToProcess = new StringBuilder();
        StringBuilder linesBuilder = new StringBuilder();
        int linesBuilderLineConter = 0;

        int indexCounter = 0;

        Map<String, Long> timeCounters = new HashMap<>();
        timeCounters.put("total", 0l);
        timeCounters.put("downloading", 0l);
        timeCounters.put("textPreProcessing", 0l);
        timeCounters.put("textProcessing", 0l);
        timeCounters.put("output", 0l);

        long startDownloading = System.currentTimeMillis();
        while (readChannel.read(bytes) > 0) {
            timeCounters.put("downloading", timeCounters.get("downloading") + (System.currentTimeMillis() - startDownloading));
            long startTextProcessing = System.currentTimeMillis();

            bytes.flip();
            String readString = StandardCharsets.UTF_8.decode(bytes).toString();
            fastqDataToProcess.append(fixLines ? removeEmptyLines(readString) : readString);
            bytes.clear();
            timeCounters.put("textPreProcessing", timeCounters.get("textPreProcessing") + (System.currentTimeMillis() - startTextProcessing));

            boolean endsWithNewLine = fastqDataToProcess.charAt(fastqDataToProcess.length() - 1) == '\n';
            List<String> lines = Stream.of(fastqDataToProcess.toString().split(NEW_LINE_INDICATION)).collect(Collectors.toList());
            fastqDataToProcess.setLength(0);

            int startOfLastFastq = ((lines.size() - 1) / FASTQ_READ_LINE_COUNT) * FASTQ_READ_LINE_COUNT;

            List<String> currentLines = new ArrayList<>(lines.subList(0, startOfLastFastq));
            List<String> tailLines = new ArrayList<>(lines.subList(startOfLastFastq, lines.size()));
            lines.clear();

            fastqDataToProcess.append(String.join(NEW_LINE_INDICATION, tailLines));
            tailLines.clear();
            if (endsWithNewLine) {
                fastqDataToProcess.append(NEW_LINE_INDICATION);
            }

            currentLines.removeAll(Arrays.asList("", null));

            for (int i = 0; i < currentLines.size(); i++) {
                currentLines.set(i, currentLines.get(i) + NEW_LINE_INDICATION);
            }

            for (int lineIndex = 0; lineIndex < currentLines.size() / FASTQ_READ_LINE_COUNT; lineIndex++) {
                for (int i = 0; i < FASTQ_READ_LINE_COUNT; i++) {
                    linesBuilder.append(currentLines.get((lineIndex * FASTQ_READ_LINE_COUNT) + i));
                }
                linesBuilderLineConter++;
                if (linesBuilderLineConter == batchSize) {

                    long finishTextProcessing = System.currentTimeMillis();
                    timeCounters.put("textProcessing", timeCounters.get("textProcessing") + (finishTextProcessing - startTextProcessing));
                    callback.onFindFastqPart(linesBuilder.toString(), indexCounter);
                    startTextProcessing = System.currentTimeMillis();
                    timeCounters.put("output", timeCounters.get("output") + (startTextProcessing - finishTextProcessing));

                    indexCounter++;

                    linesBuilder.setLength(0);
                    linesBuilderLineConter = 0;

                    LOG.info(String.format("Memory status: Total: %d, Free: %d, Diff: %d", Runtime.getRuntime().totalMemory() / (1024 * 1024),
                            Runtime.getRuntime().freeMemory() / (1024 * 1024), (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024)));

                    LOG.info(String.format("Counter: %d, TimeCounters: %s", indexCounter,
                            timeCounters.entrySet().stream()
                                    .map(stringLongEntry -> stringLongEntry.getKey() + " - " + TimeUtils.formatDeltaTime(stringLongEntry.getValue()))
                                    .collect(Collectors.joining(", "))));
                }
            }
            currentLines.clear();
            timeCounters.put("textProcessing", timeCounters.get("textProcessing") + (System.currentTimeMillis() - startTextProcessing));

            timeCounters.put("total", timeCounters.get("total") + (System.currentTimeMillis() - startDownloading));
            startDownloading = System.currentTimeMillis();
        }
        if (linesBuilderLineConter > 0) {
            callback.onFindFastqPart(linesBuilder.append(fastqDataToProcess).toString(), indexCounter);
        }
        LOG.info(String.format("Spent time ms: %d", timeCounters.get("total")));
    }

    public static interface Callback {

        void onFindFastqPart(String fastqPart, int index);
    }

}
