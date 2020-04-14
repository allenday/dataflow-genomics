package com.google.allenday.genomics.core.main.processing.split;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.processing.split.BatchSamParser;
import com.google.allenday.genomics.core.processing.SamToolsService;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchSamParserTest {

    @Test
    public void testSamRecordsBatchesStreamFromBamFile() {
        File file = new File(getClass().getClassLoader().getResource("test_sam_file.sam").getFile());

        FileUtils fileUtils = new FileUtils();
        SamToolsService samToolsService = new SamToolsService(fileUtils);
        BatchSamParser batchSamParser = new BatchSamParser(samToolsService, fileUtils);

        try {
            String destPath = samToolsService.sortSam(file.getAbsolutePath(), "", "result", "sorted");
            List<String> fileNames = new ArrayList<>();
            batchSamParser.samRecordsBatchesStreamFromBamFile(destPath, "", new BatchSamParser.BatchResultEmmiter() {
                @Override
                public void onBatchResult(String contig, long start, long end, String fileName) {
                    fileNames.add(fileName);
                    fileUtils.deleteFile(fileName);
                }
            }, 100000);
            Assert.assertEquals("Wrong result size", 25, fileNames.size());
            fileUtils.deleteFile(destPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
