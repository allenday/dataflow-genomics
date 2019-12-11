package com.google.allenday.genomics.core.main.processing;

import com.google.allenday.genomics.core.processing.SamBamManipulationService;
import com.google.allenday.genomics.core.io.FileUtils;
import htsjdk.samtools.SAMRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;


public class SamBamManipulationTests implements Serializable {
    private final static String BAM_FILE = "expected_single_end_result.merged.sorted.bam";

    @Test
    public void testSamRecordsFromBamFile() throws IOException {
        SamBamManipulationService samBamManipulationService = new SamBamManipulationService(new FileUtils());

        List<SAMRecord> samRecords = samBamManipulationService.samRecordsFromBamFile(getClass().getClassLoader().getResource(BAM_FILE).getFile());
        Assert.assertEquals(samRecords.size(), 20000);
    }
}