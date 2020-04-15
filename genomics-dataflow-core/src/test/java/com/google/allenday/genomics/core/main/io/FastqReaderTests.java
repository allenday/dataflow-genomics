package com.google.allenday.genomics.core.main.io;

import com.google.allenday.genomics.core.io.FastqReader;
import com.google.allenday.genomics.core.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class FastqReaderTests {

    private final static String TEST_FASTQ_FILE = "test_paired_read_5000_1.fastq";
    private final static String TEST_FASTQ_GZ_FILE = "test_paired_read_5000_1.fastq.gz";
    private final static int TEST_BATCH_SIZE = 1500;
    private final static int EXPECTED_BATCHES_COUNT = 4;
    private final static String EXPECTED_CHECK_LINE = "@QRJC01000008.1_44771_45188_0_1_0_0_1:0:0_3:0:0_88/1";

    @Test
    public void testReadFastqBlobWithReadCountLimit() throws IOException {
        readFastqBlobWithReadCountLimit(TEST_FASTQ_FILE);
    }

    @Test
    public void testReadGzipFastqBlobWithReadCountLimit() throws IOException {
        readFastqBlobWithReadCountLimit(TEST_FASTQ_GZ_FILE);
    }

    private void readFastqBlobWithReadCountLimit(String fileName) throws IOException {
        RandomAccessFile reader = new RandomAccessFile(getClass().getClassLoader().getResource(fileName).getFile(), "r");

        ReadableByteChannel channel = reader.getChannel();
        FastqReader fastqReader = new FastqReader();
        FileUtils fileUtils = new FileUtils();

        List<String> parts = new ArrayList<>();
        fastqReader.readFastqBlobWithReadCountLimit(fileUtils.getInputStreamFromReadChannel(fileName, channel),
                TEST_BATCH_SIZE, (fastqPart, index) -> {
                    parts.add(fastqPart);
                });
        Assert.assertEquals(EXPECTED_BATCHES_COUNT, parts.size());
        Assert.assertEquals(EXPECTED_CHECK_LINE, parts.get(parts.size() - 1).split("\n")[0]);
    }
}
