package com.google.allenday.genomics.core.processing.sam;

import com.google.allenday.genomics.core.io.FileUtils;
import htsjdk.samtools.*;
import htsjdk.samtools.util.*;
import org.javatuples.Pair;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static htsjdk.samtools.ValidationStringency.LENIENT;

public class SamBamManipulationService implements Serializable {
    private static final Log LOG = Log.getInstance(SamBamManipulationService.class);

    public final static String SORTED_BAM_FILE_SUFFIX = ".sorted.bam";
    public final static String BAM_INDEX_SUFFIX = ".bai";
    public final static String MERGE_SORTED_FILE_SUFFIX = ".merged.sorted.bam";

    private SAMFileHeader.SortOrder SORT_ORDER = SAMFileHeader.SortOrder.coordinate;
    private boolean ASSUME_SORTED = false;
    private boolean MERGE_SEQUENCE_DICTIONARIES = false;
    private boolean USE_THREADING = false;
    private List<String> COMMENT = new ArrayList<>();
    private File INTERVALS = null;

    private static final int PROGRESS_INTERVAL = 1000000;
    private FileUtils fileUtils;

    public SamBamManipulationService(FileUtils fileUtils) {
        this.fileUtils = fileUtils;
    }

    public String sortSam(String inputFilePath, String workDir,
                          String outPrefix, String outSuffix) throws IOException {
        String alignedSortedBamPath = workDir + outPrefix + "_" + outSuffix + SORTED_BAM_FILE_SUFFIX;

        final SamReader reader = samReaderFromBamFile(inputFilePath, LENIENT);

        reader.getFileHeader().setSortOrder(SAMFileHeader.SortOrder.coordinate);

        SAMFileWriter samFileWriter = new SAMFileWriterFactory()
                .makeBAMWriter(reader.getFileHeader(), false, new File(alignedSortedBamPath));

        for (SAMRecord record : reader) {
            samFileWriter.addAlignment(record);
        }
        samFileWriter.close();
        reader.close();
        return alignedSortedBamPath;
    }

    public boolean isRecordsInBamEquals(File file1, File file2) {
        final SamReader reader1 = SamReaderFactory.makeDefault().open(file1);
        final SamReader reader2 = SamReaderFactory.makeDefault().open(file2);

        List<SAMRecord> samRecords1 = reader1.iterator().toList();
        List<SAMRecord> samRecords2 = reader2.iterator().toList();

        if (samRecords1.size() != samRecords2.size()) {
            LOG.info("Sam files have difference size");
            CloserUtil.close(Arrays.asList(reader1, reader2));
            return false;
        } else {
            LOG.info("Sam files have the same size");
            for (SAMRecord samRecord : samRecords1) {
                if (!samRecords2.contains(samRecord)) {
                    CloserUtil.close(Arrays.asList(reader1, reader2));
                    return false;
                }
            }
        }
        CloserUtil.close(Arrays.asList(reader1, reader2));
        return true;
    }

    public String generateMergedFileName(String outPrefix, String outSuffix) {
        return outPrefix + "_" + outSuffix + MERGE_SORTED_FILE_SUFFIX;
    }

    public List<SAMRecord> samRecordsFromBamFile(String inputFilePath) throws IOException {
        List<SAMRecord> samRecords = new ArrayList<>();

        final SamReader reader = samReaderFromBamFile(inputFilePath, ValidationStringency.DEFAULT_STRINGENCY);
        for (SAMRecord record : reader) {
            samRecords.add(record);
        }
        reader.close();
        return samRecords;
    }

    public String samRecordsToBam(SAMFileHeader samFileHeader, String filepath, List<SAMRecord> samRecords) throws IOException {
        SAMFileWriter samFileWriter = new SAMFileWriterFactory()
                .makeBAMWriter(samFileHeader, false, new File(filepath));

        samRecords.forEach(samFileWriter::addAlignment);
        samFileWriter.close();
        return filepath;
    }

    public Pair<SAMFileHeader, Stream<SAMRecord>> samRecordsStreamFromBamFile(String inputFilePath) throws IOException {
        final SamReader reader = samReaderFromBamFile(inputFilePath, ValidationStringency.DEFAULT_STRINGENCY);
        SAMFileHeader fileHeader = reader.getFileHeader();
        return Pair.with(fileHeader, StreamSupport.stream(reader.spliterator(), false).onClose(() -> {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }));
    }


    public Stream<SAMRecord> samRecordsBatchesStreamFromBamFile(String inputFilePath) throws IOException {
        final SamReader reader = samReaderFromBamFile(inputFilePath, ValidationStringency.DEFAULT_STRINGENCY);
        reader.getFileHeader().setSortOrder(SAMFileHeader.SortOrder.coordinate);

        return StreamSupport.stream(reader.spliterator(), false).onClose(() -> {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        });
    }


    public SamReader samReaderFromBamFile(String inputFilePath, ValidationStringency validationStringency) throws IOException {
        return SamReaderFactory.makeDefault().validationStringency(validationStringency).open(new File(inputFilePath));
    }

    public String mergeBamFiles(List<String> localBamPaths, String workDir,
                                String outPrefix, String outSuffix) {
        String outputFileName = workDir + generateMergedFileName(outPrefix, outSuffix);

        List<Path> inputPaths = localBamPaths.stream().map(el -> Paths.get(el)).collect(Collectors.toList());

        boolean matchedSortOrders = true;

        // read interval list if it is defined
        final List<Interval> intervalList = (INTERVALS == null ? null : IntervalList.fromFile(INTERVALS).uniqued().getIntervals());
        // map reader->iterator used if INTERVALS is defined
        final Map<SamReader, CloseableIterator<SAMRecord>> samReaderToIterator = new HashMap<>(inputPaths.size());

        // Open the files for reading and writing
        final List<SamReader> readers = new ArrayList<>();
        final List<SAMFileHeader> headers = new ArrayList<>();
        {
            SAMSequenceDictionary dict = null; // Used to try and reduce redundant SDs in memory

            for (final Path inFile : inputPaths) {
                IOUtil.assertFileIsReadable(inFile);
                final SamReader in = SamReaderFactory.makeDefault().validationStringency(LENIENT)
                        .referenceSequence(Defaults.REFERENCE_FASTA).open(inFile);
                if (INTERVALS != null) {
                    if (!in.hasIndex()) {
                        throw new RuntimeException("Merging with interval but BAM file is not indexed: " + inFile);
                    }
                    final CloseableIterator<SAMRecord> samIterator = new SamRecordIntervalIteratorFactory().makeSamRecordIntervalIterator(in, intervalList, true);
                    samReaderToIterator.put(in, samIterator);
                }

                readers.add(in);
                headers.add(in.getFileHeader());

                // A slightly hackish attempt to keep memory consumption down when merging multiple files with
                // large sequence dictionaries (10,000s of sequences). If the dictionaries are identical, then
                // replace the duplicate copies with a single dictionary to reduce the memory footprint.
                if (dict == null) {
                    dict = in.getFileHeader().getSequenceDictionary();
                } else if (dict.equals(in.getFileHeader().getSequenceDictionary())) {
                    in.getFileHeader().setSequenceDictionary(dict);
                }

                matchedSortOrders = matchedSortOrders && in.getFileHeader().getSortOrder() == SORT_ORDER;
            }
        }

        // If all the input sort orders match the output sort order then just mergeBamFiles them and
        // write on the fly, otherwise setup to mergeBamFiles and sort before writing out the final file
        File outputFile = new File(outputFileName);
        IOUtil.assertFileIsWritable(new File(outputFileName));
        final boolean presorted;
        final SAMFileHeader.SortOrder headerMergerSortOrder;
        final boolean mergingSamRecordIteratorAssumeSorted;

        if (matchedSortOrders || SORT_ORDER == SAMFileHeader.SortOrder.unsorted || ASSUME_SORTED || INTERVALS != null) {
            LOG.info("Input files are in same order as output so sorting to temp directory is not needed.");
            headerMergerSortOrder = SORT_ORDER;
            mergingSamRecordIteratorAssumeSorted = ASSUME_SORTED;
            presorted = true;
        } else {
            LOG.info("Sorting input files using temp directory ");
            headerMergerSortOrder = SAMFileHeader.SortOrder.unsorted;
            mergingSamRecordIteratorAssumeSorted = false;
            presorted = false;
        }
        final SamFileHeaderMerger headerMerger = new SamFileHeaderMerger(headerMergerSortOrder, headers, MERGE_SEQUENCE_DICTIONARIES);
        final MergingSamRecordIterator iterator;
        // no interval defined, get an iterator for the whole bam
        if (intervalList == null) {
            iterator = new MergingSamRecordIterator(headerMerger, readers, mergingSamRecordIteratorAssumeSorted);
        } else {
            // show warning related to https://github.com/broadinstitute/picard/pull/314/files
            LOG.info("Warning: merged bams from different interval lists may contain the same read in both files");
            iterator = new MergingSamRecordIterator(headerMerger, samReaderToIterator, true);
        }
        final SAMFileHeader header = headerMerger.getMergedHeader();
        for (final String comment : COMMENT) {
            header.addComment(comment);
        }
        header.setSortOrder(SORT_ORDER);
        final SAMFileWriterFactory samFileWriterFactory = new SAMFileWriterFactory();
        if (USE_THREADING) {
            samFileWriterFactory.setUseAsyncIo(true);
        }
        final SAMFileWriter out = samFileWriterFactory.makeSAMOrBAMWriter(header, presorted, outputFile);

        // Lastly loop through and write out the records
        final ProgressLogger progress = new ProgressLogger(LOG, PROGRESS_INTERVAL);
        while (iterator.hasNext()) {
            final SAMRecord record = iterator.next();
            out.addAlignment(record);
            progress.record(record);
        }

        LOG.info("Finished reading inputs.");
        for (final CloseableIterator<SAMRecord> iter : samReaderToIterator.values()) CloserUtil.close(iter);
        CloserUtil.close(readers);
        out.close();
        return outputFileName;
    }

    public String createIndex(String inputFilePath) throws IOException {
        final SamReader reader = SamReaderFactory.makeDefault()
                .validationStringency(LENIENT)
                .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS)
                .open(new File(inputFilePath));
        String outputFilePath = inputFilePath + BAM_INDEX_SUFFIX;
        BAMIndexer.createIndex(reader, new File(outputFilePath));
        reader.close();
        return outputFilePath;
    }


    public Map<String, Long> parseIndexToContigAndLengthMap(String indexContent) {
        return Stream.of(indexContent.split("\n"))
                .map(line -> {
                    String[] elements = line.split("\t");
                    return new AbstractMap.SimpleEntry<>(elements[0], Long.parseLong(elements[1]));
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
