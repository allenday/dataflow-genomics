package com.google.allenday.genomics.core.preparing.fastq;

import com.google.allenday.genomics.core.processing.sam.SamToolsService;
import com.google.allenday.genomics.core.utils.TimeUtils;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.fastq.FastqEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FastqReader implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(FastqReader.class);

    private final static int BUFFER_SIZE = 64 * 1024;
    private final static String NEW_LINE_INDICATION = "\n";
    private final static int FASTQ_READ_LINE_COUNT = 4;

    private SamToolsService samToolsService;

    public FastqReader(SamToolsService samToolsService) {
        this.samToolsService = samToolsService;
    }

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
    public void readFastqBlobWithSizeLimit(InputStream readChannel, long batchSize, SingleFastqCallback singleFastqCallback) throws IOException {
        if (batchSize < BUFFER_SIZE) {
            batchSize = BUFFER_SIZE;
        }

        byte[] bytes = new byte[BUFFER_SIZE];
        String fastqTail = "";
        List<String> linesCollector = new ArrayList<>();

        int indexCounter = 0;
        long currentReadSize = 0;
        long timeCount = 0;

        int readCount;
        while ((readCount = readChannel.read(bytes)) > 0) {
            long start = System.currentTimeMillis();

            StringBuilder newLines = removeEmptyLines(new String(bytes, 0, readCount));

            currentReadSize += newLines.toString().getBytes().length;
            StringBuilder readString = newLines.insert(0, fastqTail);

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
                singleFastqCallback.onFindFastqPart(output, indexCounter);
                indexCounter++;

                linesCollector.clear();
                currentReadSize = fastqTail.getBytes().length;
            }
            timeCount += System.currentTimeMillis() - start;
        }
        if (linesCollector.size() > 0) {
            singleFastqCallback.onFindFastqPart(String.join("", linesCollector) + fastqTail, indexCounter);
        }
        LOG.info(String.format("Spent time ms: %d", timeCount));
    }

    public void readFastqBlobWithReadCountLimit(InputStream readChannel, int batchSize, SingleFastqCallback singleFastqCallback) throws IOException {
        readFastqBlobWithReadCountLimit(readChannel, batchSize, singleFastqCallback, false);
    }

    public void readFastqBlobWithReadCountLimit(InputStream readChannel, int batchSize, SingleFastqCallback singleFastqCallback, boolean repairLines) throws IOException {
        byte[] bytes = new byte[BUFFER_SIZE];
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
        int readCount;
        while ((readCount = readChannel.read(bytes)) > 0) {
            timeCounters.put("downloading", timeCounters.get("downloading") + (System.currentTimeMillis() - startDownloading));
            long startTextProcessing = System.currentTimeMillis();

            String readString = new String(bytes, 0, readCount);
            fastqDataToProcess.append(repairLines ? removeEmptyLines(readString) : readString);
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
                    singleFastqCallback.onFindFastqPart(linesBuilder.toString(), indexCounter);
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
            singleFastqCallback.onFindFastqPart(linesBuilder.append(fastqDataToProcess).toString(), indexCounter);
        }
        LOG.info(String.format("Spent time ms: %d", timeCounters.get("total")));
    }

    public void readFastqRecordsFromUBAM(InputStream readChannel, int batchSize, PairedFastqCallback pairedFastqCallback) throws IOException {
        int counter = 0;
        int indexCounter = 0;
        StringBuilder stringBuilderForward = new StringBuilder();
        StringBuilder stringBuilderBack = new StringBuilder();

        for (SAMRecord samRecord : samToolsService.samReaderFromInputStream(readChannel)) {
            if (!samRecord.getReadPairedFlag() || samRecord.getFirstOfPairFlag()) {
                if (counter == batchSize) {
                    List<String> fastqParts = new ArrayList<>();

                    fastqParts.add(stringBuilderForward.toString());
                    stringBuilderForward.setLength(0);

                    if (stringBuilderBack.length() > 0) {
                        fastqParts.add(stringBuilderBack.toString());
                        stringBuilderBack.setLength(0);
                    }
                    pairedFastqCallback.onFindFastqPart(new ArrayList<>(fastqParts), indexCounter);
                    fastqParts.clear();
                    counter = 0;
                    indexCounter++;
                }
                stringBuilderForward.append(FastqEncoder.asFastqRecord(samRecord).toFastQString()).append(NEW_LINE_INDICATION);
                counter++;
            } else {
                stringBuilderBack.append(FastqEncoder.asFastqRecord(samRecord).toFastQString()).append(NEW_LINE_INDICATION);
            }
        }
        List<String> fastqParts = new ArrayList<>();
        fastqParts.add(stringBuilderForward.toString());

        if (stringBuilderBack.length() > 0) {
            fastqParts.add(stringBuilderBack.toString());
        }
        pairedFastqCallback.onFindFastqPart(fastqParts, indexCounter);
    }

    public static interface SingleFastqCallback {

        void onFindFastqPart(String fastqPart, int index);
    }

    public static interface PairedFastqCallback {

        void onFindFastqPart(List<String> fastqContents, int index);
    }
}
