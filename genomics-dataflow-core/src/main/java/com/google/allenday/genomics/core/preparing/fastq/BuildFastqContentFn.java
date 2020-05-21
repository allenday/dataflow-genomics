package com.google.allenday.genomics.core.preparing.fastq;

import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.pipeline.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import htsjdk.samtools.fastq.FastqConstants;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BuildFastqContentFn extends DoFn<KV<SampleRunMetaData, Iterable<Iterable<KV<FileWrapper, Integer>>>>,
        KV<SampleRunMetaData, List<FileWrapper>>> {
    private static final Logger LOG = LoggerFactory.getLogger(BuildFastqContentFn.class);

    private TransformIoHandler buildFastqContentIoHandler;
    private FileUtils fileUtils;
    private int maxContentSizeMb;

    protected GcsService gcsService;

    public BuildFastqContentFn(TransformIoHandler buildFastqContentIoHandler, FileUtils fileUtils, int maxContentSizeMb) {
        this.buildFastqContentIoHandler = buildFastqContentIoHandler;
        this.fileUtils = fileUtils;
        this.maxContentSizeMb = maxContentSizeMb;
    }

    private KV<SampleRunMetaData, List<FileWrapper>> generateOutput(SampleRunMetaData sampleRunMetaData,
                                                                    TransformIoHandler buildFastqContentIoHandler,
                                                                    int currentSubPartIndex, List<StringBuilder> contents) {
        SampleRunMetaData sampleRunMetaDataForOutput = sampleRunMetaData.cloneWithNewSubPartIndex(currentSubPartIndex);
        List<FileWrapper> fileWrappersForOutput = new ArrayList<>();
        for (int i = 0; i < contents.size(); i++) {
            String filename = sampleRunMetaData.getRunId()
                    + String.format("_subpart_%d", currentSubPartIndex)
                    + String.format("__%d", i + 1) + FastqConstants.FastqExtensions.FASTQ.getExtension();
            try {
                FileWrapper fileWrapper = buildFastqContentIoHandler.handleContentOutput(gcsService, contents.get(i).toString().getBytes(), filename,
                        sampleRunMetaData.getRunId());
                fileWrappersForOutput.add(fileWrapper);
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
        LOG.info(String.format("Built files %s", fileWrappersForOutput.stream().map(FileWrapper::getFileName).collect(Collectors.joining(", "))));
        return KV.of(sampleRunMetaDataForOutput, fileWrappersForOutput);
    }


    @Setup
    public void setUp() {
        gcsService = GcsService.initialize(new FileUtils());
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SampleRunMetaData, Iterable<Iterable<KV<FileWrapper, Integer>>>> input = c.element();

        long start = System.currentTimeMillis();
        SampleRunMetaData sampleRunMetaData = input.getKey();
        LOG.info(String.format("Start building content of %s", sampleRunMetaData.getRunId()));
        List<StringBuilder> contents = new ArrayList<StringBuilder>() {{
            add(new StringBuilder());
        }};
        if (sampleRunMetaData.isPaired()) {
            contents.add(new StringBuilder());
        }
        int maxContentSize = (int) fileUtils.mbToBytes(maxContentSizeMb);
        int currentSubPartIndex = 0;
        for (Iterable<KV<FileWrapper, Integer>> pair : input.getValue()) {
            List<FileWrapper> fileWrappers = StreamSupport.stream(pair.spliterator(), false)
                    .sorted(Comparator.comparing(KV::getValue)).map(KV::getKey).collect(Collectors.toList());

            List<String> newContentList = fileWrappers.stream()
                    .map(fileWrapper -> new String(buildFastqContentIoHandler.handleInputAsContent(gcsService, fileWrapper)))
                    .collect(Collectors.toList());
            for (int i = 0; i < fileWrappers.size(); i++) {
                String newContent = newContentList.get(i);
                if (contents.get(i).length() + newContent.length() > maxContentSize) {
                    c.output(generateOutput(sampleRunMetaData, buildFastqContentIoHandler, currentSubPartIndex, contents));
                    currentSubPartIndex++;

                    contents.forEach(content -> content.setLength(0));
                    break;
                }
            }
            for (int i = 0; i < fileWrappers.size(); i++) {
                contents.get(i).append(newContentList.get(i));
            }

        }
        LOG.info(String.format("Finish building of %s in %d", sampleRunMetaData.getRunId(), System.currentTimeMillis() - start));
        c.output(generateOutput(sampleRunMetaData, buildFastqContentIoHandler, currentSubPartIndex, contents));
    }
}