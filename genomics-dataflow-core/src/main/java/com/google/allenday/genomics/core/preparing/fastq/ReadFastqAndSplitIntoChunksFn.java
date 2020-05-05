package com.google.allenday.genomics.core.preparing.fastq;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.runfile.FastqInputResource;
import com.google.allenday.genomics.core.preparing.runfile.SraInputResource;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.pipeline.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.processing.align.Instrument;
import com.google.allenday.genomics.core.processing.sam.SamToolsService;
import com.google.allenday.genomics.core.preparing.sra.SraToolsService;
import htsjdk.samtools.fastq.FastqConstants;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

public abstract class ReadFastqAndSplitIntoChunksFn<T>
        extends DoFn<
        KV<SampleRunMetaData, T>,
        KV<SampleRunMetaData, KV<FileWrapper, Integer>>
        > {
    private final static Logger LOG = LoggerFactory.getLogger(ReadFastqAndSplitIntoChunksFn.class);

    protected FileUtils fileUtils;
    protected FastqReader fastqReader;
    protected int chunkSizeCount;
    protected boolean hasFastqChunkByteSizeLimitation;
    protected TransformIoHandler splitFastqIntoBatchesIoHandler;

    GcsService gcsService;

    public ReadFastqAndSplitIntoChunksFn(FileUtils fileUtils, FastqReader fastqReader,
                                         TransformIoHandler splitFastqIntoBatchesIoHandler, int chunkSizeCount,
                                         boolean hasFastqChunkByteSizeLimitation) {
        this.fileUtils = fileUtils;
        this.fastqReader = fastqReader;
        this.chunkSizeCount = chunkSizeCount;
        this.splitFastqIntoBatchesIoHandler = splitFastqIntoBatchesIoHandler;
        this.hasFastqChunkByteSizeLimitation = hasFastqChunkByteSizeLimitation;
    }

    protected void workWithFastqInputStream(SampleRunMetaData sampleRunMetaData,
                                            String fileName,
                                            InputStream inputStream,
                                            int pairIndex,
                                            Callback callback) throws IOException {
        Pair<String, String> baseAndExtension = fileUtils.splitFilenameAndExtension(fileName);
        String fileNameBase = baseAndExtension.getValue0();
        String srcExtension = baseAndExtension.getValue1();

        LOG.info(String.format("Working with %s", fileName));
        if (srcExtension.endsWith(SamToolsService.BAM_FILE_EXTENSION)) {
            FastqReader.PairedFastqCallback pairedFastqCallback = (fastqParts, index) -> {
                SampleRunMetaData indexedSampleRunMetaData = sampleRunMetaData.cloneWithNewPartIndex(index);
                for (String fastqPart : fastqParts) {
                    String filename = fileNameBase + "_" + (fastqParts.indexOf(fastqPart) + 1) +
                            "_" + index + FastqConstants.FastqExtensions.FASTQ.getExtension();
                    try {
                        FileWrapper indexedFileWrapper =
                                splitFastqIntoBatchesIoHandler.handleContentOutput(gcsService, fastqPart.getBytes(), filename);
                        callback.onResult(KV.of(indexedSampleRunMetaData, KV.of(indexedFileWrapper, pairIndex)));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };
            fastqReader.readFastqRecordsFromUBAM(inputStream, chunkSizeCount, pairedFastqCallback);
        } else {
            FastqReader.SingleFastqCallback singleFastqCallback = (fastqPart, index) -> {
                LOG.info(String.format("Receive new part of %s with index %d, size %s",
                        fileName, index, fastqPart.getBytes().length));

                SampleRunMetaData indexedSampleRunMetaData = sampleRunMetaData.cloneWithNewPartIndex(index);
                String filename = fileNameBase + "_" + index + FastqConstants.FastqExtensions.FASTQ.getExtension();
                try {
                    FileWrapper indexedFileWrapper =
                            splitFastqIntoBatchesIoHandler.handleContentOutput(gcsService, fastqPart.getBytes(), filename);
                    callback.onResult(KV.of(indexedSampleRunMetaData, KV.of(indexedFileWrapper, pairIndex)));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            };
            fastqReader.readFastqBlobWithReadCountLimit(inputStream, chunkSizeCount, singleFastqCallback);

        }
    }

    @Setup
    public void setUp() {
        gcsService = GcsService.initialize(new FileUtils());
        additionalSetup();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        if (hasFastqChunkByteSizeLimitation) {
            splitFastqIntoBatchesIoHandler.setMemoryOutputLimitMb(0);
        }
        SampleRunMetaData sampleRunMetaData = c.element().getKey();
        Instrument instrument;
        try {
            instrument = Instrument.valueOf(sampleRunMetaData.getPlatform());
        } catch (IllegalArgumentException e) {
            LOG.error(e.getMessage());
            return;
        }
        chunkSizeCount = chunkSizeCount / instrument.sizeMultiplier;
        if (chunkSizeCount < 1) {
            chunkSizeCount = 1;
        }

        processElement(c, sampleRunMetaData, c.element().getValue());
    }

    public interface Callback {
        void onResult(KV<SampleRunMetaData, KV<FileWrapper, Integer>> result);
    }

    protected abstract void additionalSetup();

    protected abstract void processElement(ProcessContext c, SampleRunMetaData sampleRunMetaData, T input);


    public static class FromFastqInputResource extends ReadFastqAndSplitIntoChunksFn<KV<FastqInputResource, Integer>> {

        public FromFastqInputResource(FileUtils fileUtils, FastqReader fastqReader,
                                      TransformIoHandler splitFastqIntoBatchesIoHandler,
                                      int chunkSizeCount, boolean hasFastqChunkByteSizeLimitation) {
            super(fileUtils, fastqReader, splitFastqIntoBatchesIoHandler, chunkSizeCount, hasFastqChunkByteSizeLimitation);
        }

        @Override
        protected void additionalSetup() {

        }

        @Override
        protected void processElement(ProcessContext c,
                                      SampleRunMetaData sampleRunMetaData,
                                      KV<FastqInputResource, Integer> input) {
            FastqInputResource fastqInputResource = input.getKey();
            Integer pairIndex = input.getValue();
            try {

                InputStream inputStream = fastqInputResource.getInputStream(fileUtils, gcsService);

                String fileName = fileUtils.getFilenameFromPath(fastqInputResource.getName());
                workWithFastqInputStream(sampleRunMetaData, fileName, inputStream, pairIndex, c::output);

            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    public static class FromSraInputResource extends ReadFastqAndSplitIntoChunksFn<SraInputResource> {

        private SraToolsService sraToolsService;

        public FromSraInputResource(FileUtils fileUtils,
                                    FastqReader fastqReader,
                                    TransformIoHandler splitFastqIntoBatchesIoHandler,
                                    SraToolsService sraToolsService,
                                    int chunkSizeCount,
                                    boolean hasFastqChunkByteSizeLimitation) {
            super(fileUtils, fastqReader, splitFastqIntoBatchesIoHandler, chunkSizeCount, hasFastqChunkByteSizeLimitation);
            this.sraToolsService = sraToolsService;
        }

        @Override
        protected void additionalSetup() {
            sraToolsService.setup();
        }

        @Override
        protected void processElement(ProcessContext c,
                                      SampleRunMetaData sampleRunMetaData,
                                      SraInputResource input) {
            String workingDir = fileUtils.makeDirByCurrentTimestampAndSuffix(sampleRunMetaData.getRunId());
            try {
                List<String> fastqFilePaths = sraToolsService.retrieveSraFromFastq(sampleRunMetaData.getRunId(), workingDir)
                        .stream().sorted(String::compareTo).collect(Collectors.toList());
                for (String fastqFile : fastqFilePaths) {
                    InputStream inputStream = fileUtils.getInputStreamFromFile(fastqFile);
                    String fileName = fileUtils.getFilenameFromPath(fastqFile);

                    workWithFastqInputStream(sampleRunMetaData, fileName, inputStream, fastqFilePaths.indexOf(fastqFile), c::output);
                }
                fileUtils.deleteDir(workingDir);
            } catch (IOException e) {
                LOG.error(e.getMessage());
                fileUtils.deleteDir(workingDir);
            }
        }
    }
}
