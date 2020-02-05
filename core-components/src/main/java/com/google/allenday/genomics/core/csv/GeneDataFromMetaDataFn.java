package com.google.allenday.genomics.core.csv;

import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.io.UriProvider;
import com.google.allenday.genomics.core.io.FileUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class GeneDataFromMetaDataFn extends DoFn<SampleMetaData, KV<SampleMetaData, List<FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(GeneDataFromMetaDataFn.class);
    private UriProvider uriProvider;
    private FileUtils fileUtils;

    public GeneDataFromMetaDataFn(UriProvider uriProvider, FileUtils fileUtils) {
        this.uriProvider = uriProvider;
        this.fileUtils = fileUtils;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        SampleMetaData input = c.element();
        LOG.info(String.format("GeneDataFromMetaDataFn %s", input.toString()));

        List<FileWrapper> fileWrapperList = uriProvider.provide(input).stream()
                .map(uri -> FileWrapper.fromBlobUri(uri, fileUtils.getFilenameFromPath(uri))).collect(Collectors.toList());
        c.output(KV.of(input, fileWrapperList));
    }
}
