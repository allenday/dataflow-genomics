package com.google.allenday.genomics.core.processing;

import com.google.allenday.genomics.core.io.FastqReader;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.cloud.ReadChannel;
import htsjdk.samtools.util.Log;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class SplitFastqIntoBatches extends PTransform<PCollection<KV<SampleMetaData, List<FileWrapper>>>,
        PCollection<KV<SampleMetaData, List<FileWrapper>>>> {

    private ReadFastqPartFn readFastqPartFn;
    private BuildFastqContentFn buildFastqContentFn;

    public SplitFastqIntoBatches(ReadFastqPartFn readFastqPartFn, BuildFastqContentFn buildFastqContentFn) {
        this.readFastqPartFn = readFastqPartFn;
        this.buildFastqContentFn = buildFastqContentFn;
    }

    @Override
    public PCollection<KV<SampleMetaData, List<FileWrapper>>> expand(
            PCollection<KV<SampleMetaData, List<FileWrapper>>> input) {
        return input
                .apply("Split pairs files into separate threads", FlatMapElements
                        .via(new InferableFunction<KV<SampleMetaData, List<FileWrapper>>,
                                Iterable<KV<KV<SampleMetaData, String>, KV<FileWrapper, Integer>>>>() {
                            @Override
                            public Iterable<KV<KV<SampleMetaData, String>, KV<FileWrapper, Integer>>> apply(KV<SampleMetaData, List<FileWrapper>> input) throws Exception {
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
                .apply("Group by part index", GroupByKey.create())
                .apply(MapElements.via(new SimpleFunction<KV<SampleMetaData, Iterable<KV<FileWrapper, Integer>>>, KV<SampleMetaData, Iterable<KV<FileWrapper, Integer>>>>() {
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
    }

    public static class ReadFastqPartFn extends DoFn<KV<KV<SampleMetaData, String>, Iterable<KV<FileWrapper, Integer>>>,
            KV<SampleMetaData, KV<FileWrapper, Integer>>> {
        private static final Log LOG = Log.getInstance(ReadFastqPartFn.class);

        private FileUtils fileUtils;
        private FastqReader fastqReader;
        private int chunkSizeCount;

        GCSService gcsService;

        public ReadFastqPartFn(FileUtils fileUtils, FastqReader fastqReader, int chunkSizeCount) {
            this.fileUtils = fileUtils;
            this.fastqReader = fastqReader;
            this.chunkSizeCount = chunkSizeCount;
        }

        @Setup
        public void setUp() {
            gcsService = GCSService.initialize(new FileUtils());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<KV<SampleMetaData, String>, Iterable<KV<FileWrapper, Integer>>> element = c.element();
            Optional<KV<FileWrapper, Integer>> fileWrapper = StreamSupport.stream(element.getValue().spliterator(), false).findFirst();

            SampleMetaData sampleMetaData = element.getKey().getKey();
            if (sampleMetaData != null) {
                fileWrapper.ifPresent(fileWrapperIntegerKV -> {
                    FileWrapper fastqFW = fileWrapperIntegerKV.getKey();
                    int pairIndex = fileWrapperIntegerKV.getValue();

                    Pair<String, String> baseAndExtension = fileUtils.splitFilenameAndExtension(fastqFW.getFileName());

                    if (fastqFW.getDataType() == FileWrapper.DataType.BLOB_URI) {
                        ReadChannel blobReader = gcsService.getBlobReader(gcsService.getBlobIdFromUri(fastqFW.getBlobUri()));

                        try {
                            FastqReader.Callback callback = (fastqPart, index) -> {
                                LOG.info(String.format("Receive new part of %s with index %d, size %s",
                                        fastqFW.getBlobUri(), index, fastqPart.getBytes().length));

                                SampleMetaData indexedSampleMetaData = sampleMetaData.cloneWithNewPartIndex(index);

                                FileWrapper indexedFileWrapper = FileWrapper.fromByteArrayContent(fastqPart.getBytes(),
                                        baseAndExtension.getValue0() + "_" + index + baseAndExtension.getValue1());
                                c.output(KV.of(indexedSampleMetaData, KV.of(indexedFileWrapper, pairIndex)));
                            };
                            fastqReader.readFastqBlobWithReadCountLimit(blobReader, chunkSizeCount, callback);
                        } catch (IOException e) {
                            LOG.error(e);
                        }
                    }
                });
            }
        }
    }

    public static class BuildFastqContentFn extends DoFn<KV<SampleMetaData, Iterable<Iterable<KV<FileWrapper, Integer>>>>,
            KV<SampleMetaData, List<FileWrapper>>> {
        private static final Log LOG = Log.getInstance(BuildFastqContentFn.class);

        private int maxContentSizeMb;

        public BuildFastqContentFn(int maxContentSizeMb) {
            this.maxContentSizeMb = maxContentSizeMb;
        }

        private KV<SampleMetaData, List<FileWrapper>> generateOutput(SampleMetaData sampleMetaData,
                                                                     int currentSubPartIndex, List<StringBuilder> contents) {
            SampleMetaData sampleMetaDataForOutput = sampleMetaData.cloneWithNewSubPartIndex(currentSubPartIndex);
            List<FileWrapper> fileWrappersForOutput = new ArrayList<>();
            for (int pairIndex = 1; pairIndex < contents.size() + 1; pairIndex++) {
                fileWrappersForOutput.add(FileWrapper.fromByteArrayContent(contents.get(pairIndex - 1).toString().getBytes(),
                        sampleMetaData.getRunId()
                                + String.format("_subpart_%d", currentSubPartIndex)
                                + String.format("__%d.fastq", pairIndex)));
            }
            LOG.info(String.format("Built files %s", fileWrappersForOutput.stream().map(FileWrapper::getFileName).collect(Collectors.joining(", "))));
            return KV.of(sampleMetaDataForOutput, fileWrappersForOutput);
        }

        private int mbToBytes(int mbValue){
            return mbValue * 1000 * 1000;
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
            int maxContentSize = mbToBytes(maxContentSizeMb);
            int currentSubPartIndex = 0;
            for (Iterable<KV<FileWrapper, Integer>> pair : input.getValue()) {
                List<FileWrapper> fileWrappers = StreamSupport.stream(pair.spliterator(), false)
                        .sorted(Comparator.comparing(KV::getValue)).map(KV::getKey).collect(Collectors.toList());

                for (int i = 0; i < fileWrappers.size(); i++) {
                    String newContent = new String(fileWrappers.get(i).getContent());
                    if (contents.get(i).length() + newContent.length() > maxContentSize) {
                        c.output(generateOutput(sampleMetaData, currentSubPartIndex, contents));
                        currentSubPartIndex++;

                        contents.forEach(content -> content.setLength(0));
                        break;
                    }
                }
                for (int i = 0; i < fileWrappers.size(); i++) {
                    String newContent = new String(fileWrappers.get(i).getContent());
                    contents.get(i).append(newContent);
                }

            }
            LOG.info(String.format("Finish building of %s in %d", sampleMetaData.getRunId(), System.currentTimeMillis() - start));
            c.output(generateOutput(sampleMetaData, currentSubPartIndex, contents));
        }
    }
}
