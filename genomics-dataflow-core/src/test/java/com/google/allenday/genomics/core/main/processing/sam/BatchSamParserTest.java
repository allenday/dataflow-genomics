package com.google.allenday.genomics.core.main.processing.sam;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.processing.sam.BatchSamParser;
import com.google.allenday.genomics.core.processing.sam.SamBamManipulationService;
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
        SamBamManipulationService samBamManipulationService = new SamBamManipulationService(fileUtils);
        BatchSamParser batchSamParser = new BatchSamParser(samBamManipulationService, fileUtils);

        try {
            String destPath = samBamManipulationService.sortSam(file.getAbsolutePath(), "", "result", "sorted");
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
