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

    public SplitFastqIntoBatches(ReadFastqPartFn readFastqPartFn) {
        this.readFastqPartFn = readFastqPartFn;
    }

    @Override
    public PCollection<KV<SampleMetaData, List<FileWrapper>>> expand(
            PCollection<KV<SampleMetaData, List<FileWrapper>>> input) {
        return input
                .apply(FlatMapElements
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
                .apply("Split pairs files into separate threads", GroupByKey.create())
                .apply(ParDo.of(readFastqPartFn))
                .apply("Group by part index", GroupByKey.create())
                .apply(MapElements
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

    public static class ReadFastqPartFn extends DoFn<KV<KV<SampleMetaData, String>, Iterable<KV<FileWrapper, Integer>>>, KV<SampleMetaData, KV<FileWrapper, Integer>>> {
        private static final Log LOG = Log.getInstance(ReadFastqPartFn.class);

        private FileUtils fileUtils;
        private FastqReader fastqReader;
        private long batchSizeMB;
        private int batchSizeCount;

        GCSService gcsService;

        private ReadFastqPartFn(FileUtils fileUtils, FastqReader fastqReader, long batchSizeMB, int batchSizeCount) {
            this.fileUtils = fileUtils;
            this.fastqReader = fastqReader;
            this.batchSizeMB = batchSizeMB;
            this.batchSizeCount = batchSizeCount;
        }

        public static ReadFastqPartFn withSizeLimit(FileUtils fileUtils, FastqReader fastqReader, long batchSizeMB) {
            return new ReadFastqPartFn(fileUtils, fastqReader, batchSizeMB, 0);
        }


        public static ReadFastqPartFn withCountLimit(FileUtils fileUtils, FastqReader fastqReader, int batchSizeCount) {
            return new ReadFastqPartFn(fileUtils, fastqReader, 0, batchSizeCount);
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
                            if (batchSizeCount > 0) {
                                fastqReader.readFastqBlobWithReadCountLimit(blobReader, batchSizeCount, callback);
                            } else {
                                long batchSize = batchSizeMB * 1024 * 1024;
                                fastqReader.readFastqBlobWithSizeLimit(blobReader, batchSize, callback);
                            }
                        } catch (IOException e) {
                            LOG.error(e);
                        }
                    }
                });
            }
        }
    }
}
