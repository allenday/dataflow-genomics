package com.google.allenday.genomics.core.main.processing;

import com.google.allenday.genomics.core.processing.SamToolsService;
import com.google.allenday.genomics.core.io.FileUtils;
import htsjdk.samtools.SAMRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;


public class SamToolsServiceTests implements Serializable {

    private final static String BAM_FILE = "expected_result_15k.merged.sorted.bam";
    private final static int BAM_FILE_LINES_SIZE = 15000;

    @Test
    public void testSamRecordsFromBamFile() throws IOException {
        SamToolsService samToolsService = new SamToolsService(new FileUtils());

        List<SAMRecord> samRecords = samToolsService.samRecordsFromBamFile(getClass().getClassLoader().getResource(BAM_FILE).getFile());
        Assert.assertEquals(BAM_FILE_LINES_SIZE, samRecords.size());
    }
}