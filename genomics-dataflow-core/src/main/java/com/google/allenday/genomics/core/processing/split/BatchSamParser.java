package com.google.allenday.genomics.core.processing.split;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.processing.SamToolsService;
import com.google.allenday.genomics.core.utils.StringUtils;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.commons.lang3.mutable.MutableObject;
import org.javatuples.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BatchSamParser implements Serializable {

    private SamToolsService samToolsService;
    private FileUtils fileUtils;

    public BatchSamParser(SamToolsService samToolsService, FileUtils fileUtils) {
        this.samToolsService = samToolsService;
        this.fileUtils = fileUtils;
    }

    public void samRecordsBatchesStreamFromBamFile(String inputFilePath, String workDir,
                                                   BatchResultEmmiter batchResultEmmiter,
                                                   int batchSize) throws IOException {
        String fileNameBase = fileUtils.splitFilenameToBaseAndExtension(fileUtils.getFilenameFromPath(inputFilePath)).getValue0();

        SortedMap<Integer, List<SAMRecord>> batches = new TreeMap<>();
        MutableObject<String> lastConting = new MutableObject<>();
        Pair<SAMFileHeader, Stream<SAMRecord>> headerAndStream = samToolsService.samRecordsStreamFromBamFile(inputFilePath);
        SAMFileHeader samFileHeader = headerAndStream.getValue0();
        headerAndStream.getValue1().forEach(samRecord -> {
            int start = samRecord.getStart();
            int end = samRecord.getEnd();
            String contig = samRecord.getReferenceName();

            if (lastConting.getValue() == null) {
                lastConting.setValue(contig);
            }

            if (!lastConting.getValue().equals(contig)) {
                outputData(batchResultEmmiter, samFileHeader, fileNameBase, workDir, lastConting.getValue(), batches, batchSize);
                lastConting.setValue(contig);
            }

            int startIndex = start / batchSize;
            int endIndex = end / batchSize;
            List<Integer> indexesToOutput = batches.keySet().stream().filter(key -> key < startIndex).collect(Collectors.toList());
            if (indexesToOutput.size() > 0) {
                outputData(batchResultEmmiter, samFileHeader, fileNameBase, workDir, lastConting.getValue(),
                        batches, indexesToOutput, batchSize);
            }

            IntStream.range(startIndex, endIndex + 1).forEach(i -> {
                if (!batches.containsKey(i)) {
                    batches.put(i, new ArrayList<>());
                }
                batches.get(i).add(samRecord);
            });
        });
        outputData(batchResultEmmiter, samFileHeader, fileNameBase, workDir, lastConting.getValue(), batches, batchSize);
    }


    private void outputData(BatchResultEmmiter batchResultEmmiter, SAMFileHeader samFileHeader, String fileNameBase, String workDir,
                            String contig, Map<Integer, List<SAMRecord>> batches, int batchSize) {
        outputData(batchResultEmmiter, samFileHeader, fileNameBase, workDir, contig, batches, new ArrayList<>(batches.keySet()), batchSize);
    }

    private void outputData(BatchResultEmmiter batchResultEmmiter, SAMFileHeader samFileHeader, String fileNameBase, String workDir,
                            String contig, Map<Integer, List<SAMRecord>> batches, List<Integer> batchIndexes, int batchSize) {
        batchIndexes.forEach(index -> {
            long start = (long) (index * batchSize) + 1;
            long end = (long) (index + 1) * batchSize;

            String fileName = workDir + fileNameBase + "_"
                    + StringUtils.generateSlug(contig.equals("*") ? "not_mapped" : contig)
                    + "_" + start + "_" + end + SamToolsService.SORTED_BAM_FILE_SUFFIX;
            try {
                samToolsService.samRecordsToBam(samFileHeader, fileName, batches.get(index));
                batchResultEmmiter.onBatchResult(contig, start, end, fileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        batchIndexes.forEach(batches::remove);
    }


    public interface BatchResultEmmiter {
        public void onBatchResult(String contig, long start, long end, String batchBamFile);
    }
}
