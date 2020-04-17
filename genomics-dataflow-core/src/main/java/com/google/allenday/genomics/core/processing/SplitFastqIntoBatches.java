package com.google.allenday.genomics.core.processing;

import com.google.allenday.genomics.core.io.*;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.Instrument;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.cloud.ReadChannel;
import htsjdk.samtools.fastq.FastqConstants;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Provides FASTQ splitting mechanism to increase parallelism and balance load between workers
 */
public class SplitFastqIntoBatches extends PTransform<PCollection<KV<SampleMetaData, List<FileWrapper>>>,
        PCollection<KV<SampleMetaData, List<FileWrapper>>>> {

    private ReadFastqPartFn readFastqPartFn;
    private BuildFastqContentFn buildFastqContentFn;
    private long maxContentSizeMb;

    public SplitFastqIntoBatches(ReadFastqPartFn readFastqPartFn, BuildFastqContentFn buildFastqContentFn, long maxContentSizeMb) {
        this.readFastqPartFn = readFastqPartFn;
        this.buildFastqContentFn = buildFastqContentFn;
        this.maxContentSizeMb = maxContentSizeMb;
    }

    @Override
    public PCollection<KV<SampleMetaData, List<FileWrapper>>> expand(
            PCollection<KV<SampleMetaData, List<FileWrapper>>> input) {
        PCollection<KV<SampleMetaData, Iterable<KV<FileWrapper, Integer>>>> grouppedByChunks = input
                .apply("Split pairs files into separate threads", FlatMapElements
                        .via(new InferableFunction<KV<SampleMetaData, List<FileWrapper>>,
                                Iterable<KV<KV<SampleMetaData, String>, KV<FileWrapper, Integer>>>>() {
                            @Override
                            public Iterable<KV<KV<SampleMetaData, String>, KV<FileWrapper, Integer>>> apply(KV<SampleMetaData, List<FileWrapper>> input) {
                                List<KV<KV<SampleMetaData, String>, KV<FileWrapper, Integer>>> output = new ArrayList<>();

                                IntStream.range(0, input.getValue().size())
                                        .forEach(index -> {
                                            FileWrapper fileWrapper = input.getValue().get(index);
                                            output.add(KV.of(KV.of(input.getKey(), fileWrapper.getFileName()), KV.of(fileWrapper, index)));
                                        });
                                return output;
                            }
                        }))
                .apply("Group by fastq URI", GroupByKey.create())
                .apply(ParDo.of(readFastqPartFn))
                .apply("Group by part index", GroupByKey.create());
        if (maxContentSizeMb > 0) {
            return grouppedByChunks.apply(MapElements.via(new SimpleFunction<KV<SampleMetaData, Iterable<KV<FileWrapper, Integer>>>, KV<SampleMetaData, Iterable<KV<FileWrapper, Integer>>>>() {
                @Override
                public KV<SampleMetaData, Iterable<KV<FileWrapper, Integer>>> apply(KV<SampleMetaData, Iterable<KV<FileWrapper, Integer>>> input) {
                    SampleMetaData sampleMetaData = input.getKey();
                    Iterable<KV<FileWrapper, Integer>> value = input.getValue();
                    return KV.of(sampleMetaData.cloneWithNewPartIndex(0), value);
                }
            }))
                    .apply("Group group chunks by original fastq", GroupByKey.create())
                    .apply(ParDo.of(buildFastqContentFn))
                    .apply("Split into separate threads", GroupByKey.create())
                    .apply("Get first element from iterable", MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptor.of(SampleMetaData.class), TypeDescriptors.lists(TypeDescriptor.of(FileWrapper.class))))
                            .via(kv -> KV.of(kv.getKey(), kv.getValue().iterator().next()
                            )));
        } else {
            return grouppedByChunks.apply(MapElements
                    .via(new SimpleFunction<KV<SampleMetaData, Iterable<KV<FileWrapper, Integer>>>,
                            KV<SampleMetaData, List<FileWrapper>>>() {
                        @Override
                        public KV<SampleMetaData, List<FileWrapper>> apply(KV<SampleMetaData, Iterable<KV<FileWrapper, Integer>>> input) {
                            List<FileWrapper> fileWrappers = StreamSupport.stream(input.getValue().spliterator(), false)
                                    .sorted(Comparator.comparing(KV::getValue)).map(KV::getKey).collect(Collectors.toList());
                            return KV.of(input.getKey(), fileWrappers);
                        }
                    }));
        }
    }

    public static class ReadFastqPartFn extends DoFn<KV<KV<SampleMetaData, String>, Iterable<KV<FileWrapper, Integer>>>,
            KV<SampleMetaData, KV<FileWrapper, Integer>>> {
        private final static Logger LOG = LoggerFactory.getLogger(ReadFastqPartFn.class);

        private FileUtils fileUtils;
        private FastqReader fastqReader;
        private int chunkSizeCount;
        private int maxFastqSizeMB;
        private TransformIoHandler splitFastqIntoBatchesIoHandler;

        GCSService gcsService;

        public ReadFastqPartFn(FileUtils fileUtils, FastqReader fastqReader,
                               TransformIoHandler splitFastqIntoBatchesIoHandler, int chunkSizeCount,
                               int maxFastqSizeMB) {
            this.fileUtils = fileUtils;
            this.fastqReader = fastqReader;
            this.chunkSizeCount = chunkSizeCount;
            this.splitFastqIntoBatchesIoHandler = splitFastqIntoBatchesIoHandler;
            this.maxFastqSizeMB = maxFastqSizeMB;
        }

        @Setup
        public void setUp() {
            gcsService = GCSService.initialize(new FileUtils());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<KV<SampleMetaData, String>, Iterable<KV<FileWrapper, Integer>>> element = c.element();
            Optional<KV<FileWrapper, Integer>> fileWrapper = StreamSupport.stream(element.getValue().spliterator(), false).findFirst();

            if (maxFastqSizeMB > 0) {
                splitFastqIntoBatchesIoHandler.setMemoryOutputLimitMb(0);
            }

            SampleMetaData sampleMetaData = element.getKey().getKey();

            if (sampleMetaData != null) {
                Instrument instrument;
                try {
                    instrument = Instrument.valueOf(sampleMetaData.getPlatform());
                } catch (IllegalArgumentException e) {
                    LOG.error(e.getMessage());
                    return;
                }
                chunkSizeCount = chunkSizeCount / instrument.sizeMultiplier;
                if (chunkSizeCount < 1) {
                    chunkSizeCount = 1;
                }
                fileWrapper.ifPresent(fileWrapperIntegerKV -> {
                    FileWrapper fastqFW = fileWrapperIntegerKV.getKey();
                    int pairIndex = fileWrapperIntegerKV.getValue();

                    Pair<String, String> baseAndExtension = fileUtils.splitFilenameAndExtension(fastqFW.getFileName());
                    String fileNameBase = baseAndExtension.getValue0();
                    String srcExtension = baseAndExtension.getValue1();

                    if (fastqFW.getDataType() == FileWrapper.DataType.BLOB_URI) {
                        ReadChannel blobReader = gcsService.getBlobReadChannel(gcsService.getBlobIdFromUri(fastqFW.getBlobUri()));
                        LOG.info(String.format("Working with %s", fastqFW.getBlobUri()));
                        try {
                            InputStream inputStream = fileUtils.getInputStreamFromReadChannel(fastqFW.getBlobUri(), blobReader);
                            if (srcExtension.endsWith(SamToolsService.BAM_FILE_EXTENSION)) {
                                FastqReader.PairedFastqCallback pairedFastqCallback = (fastqParts, index) -> {
                                    SampleMetaData indexedSampleMetaData = sampleMetaData.cloneWithNewPartIndex(index);
                                    for (String fastqPart : fastqParts) {
                                        String filename = fileNameBase + "_" + (fastqParts.indexOf(fastqPart) + 1) +
                                                "_" + index + FastqConstants.FastqExtensions.FASTQ.getExtension();
                                        try {
                                            FileWrapper indexedFileWrapper =
                                                    splitFastqIntoBatchesIoHandler.handleContentOutput(gcsService, fastqPart.getBytes(), filename);
                                            c.output(KV.of(indexedSampleMetaData, KV.of(indexedFileWrapper, pairIndex)));
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                };
                                fastqReader.readFastqRecordsFromUBAM(inputStream, chunkSizeCount, pairedFastqCallback);
                            } else {
                                FastqReader.SingleFastqCallback singleFastqCallback = (fastqPart, index) -> {
                                    LOG.info(String.format("Receive new part of %s with index %d, size %s",
                                            fastqFW.getBlobUri(), index, fastqPart.getBytes().length));

                                    SampleMetaData indexedSampleMetaData = sampleMetaData.cloneWithNewPartIndex(index);
                                    String filename = fileNameBase + "_" + index + FastqConstants.FastqExtensions.FASTQ.getExtension();
                                    try {
                                        FileWrapper indexedFileWrapper =
                                                splitFastqIntoBatchesIoHandler.handleContentOutput(gcsService, fastqPart.getBytes(), filename);
                                        c.output(KV.of(indexedSampleMetaData, KV.of(indexedFileWrapper, pairIndex)));
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                };
                                fastqReader.readFastqBlobWithReadCountLimit(inputStream, chunkSizeCount, singleFastqCallback);

                            }
                        } catch (IOException e) {
                            LOG.error(e.getMessage());
                        }
                    }
                });
            }
        }
    }

    public static class BuildFastqContentFn extends DoFn<KV<SampleMetaData, Iterable<Iterable<KV<FileWrapper, Integer>>>>,
            KV<SampleMetaData, List<FileWrapper>>> {
        private static final Logger LOG = LoggerFactory.getLogger(BuildFastqContentFn.class);

        private TransformIoHandler buildFastqContentIoHandler;
        private FileUtils fileUtils;
        private IoUtils ioUtils;
        private int maxContentSizeMb;

        GCSService gcsService;

        public BuildFastqContentFn(TransformIoHandler buildFastqContentIoHandler, FileUtils fileUtils, IoUtils ioUtils, int maxContentSizeMb) {
            this.buildFastqContentIoHandler = buildFastqContentIoHandler;
            this.fileUtils = fileUtils;
            this.ioUtils = ioUtils;
            this.maxContentSizeMb = maxContentSizeMb;
        }

        private KV<SampleMetaData, List<FileWrapper>> generateOutput(SampleMetaData sampleMetaData,
                                                                     TransformIoHandler buildFastqContentIoHandler,
                                                                     int currentSubPartIndex, List<StringBuilder> contents) {
            SampleMetaData sampleMetaDataForOutput = sampleMetaData.cloneWithNewSubPartIndex(currentSubPartIndex);
            List<FileWrapper> fileWrappersForOutput = new ArrayList<>();
            for (int i = 0; i < contents.size(); i++) {
                String filename = sampleMetaData.getRunId()
                        + String.format("_subpart_%d", currentSubPartIndex)
                        + String.format("__%d", i + 1) + FastqConstants.FastqExtensions.FASTQ.getExtension();
                try {
                    FileWrapper fileWrapper = buildFastqContentIoHandler.handleContentOutput(gcsService, contents.get(i).toString().getBytes(), filename);
                    fileWrappersForOutput.add(fileWrapper);
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            }
            LOG.info(String.format("Built files %s", fileWrappersForOutput.stream().map(FileWrapper::getFileName).collect(Collectors.joining(", "))));
            return KV.of(sampleMetaDataForOutput, fileWrappersForOutput);
        }


        @Setup
        public void setUp() {
            gcsService = GCSService.initialize(new FileUtils());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<SampleMetaData, Iterable<Iterable<KV<FileWrapper, Integer>>>> input = c.element();

            long start = System.currentTimeMillis();
            SampleMetaData sampleMetaData = input.getKey();
            LOG.info(String.format("Start building content of %s", sampleMetaData.getRunId()));
            List<StringBuilder> contents = new ArrayList<StringBuilder>() {{
                add(new StringBuilder());
            }};
            if (sampleMetaData.isPaired()) {
                contents.add(new StringBuilder());
            }
            int maxContentSize = (int) fileUtils.mbToBytes(maxContentSizeMb);
            int currentSubPartIndex = 0;
            for (Iterable<KV<FileWrapper, Integer>> pair : input.getValue()) {
                List<FileWrapper> fileWrappers = StreamSupport.stream(pair.spliterator(), false)
                        .sorted(Comparator.comparing(KV::getValue)).map(KV::getKey).collect(Collectors.toList());

                List<String> newContentList = fileWrappers.stream()
                        .map(fileWrapper -> new String(buildFastqContentIoHandler.handleInputAsContent(gcsService, fileWrapper, ioUtils)))
                        .collect(Collectors.toList());
                for (int i = 0; i < fileWrappers.size(); i++) {
                    String newContent = newContentList.get(i);
                    if (contents.get(i).length() + newContent.length() > maxContentSize) {
                        c.output(generateOutput(sampleMetaData, buildFastqContentIoHandler, currentSubPartIndex, contents));
                        currentSubPartIndex++;

                        contents.forEach(content -> content.setLength(0));
                        break;
                    }
                }
                for (int i = 0; i < fileWrappers.size(); i++) {
                    contents.get(i).append(newContentList.get(i));
                }

            }
            LOG.info(String.format("Finish building of %s in %d", sampleMetaData.getRunId(), System.currentTimeMillis() - start));
            c.output(generateOutput(sampleMetaData, buildFastqContentIoHandler, currentSubPartIndex, contents));
        }
    }
}
