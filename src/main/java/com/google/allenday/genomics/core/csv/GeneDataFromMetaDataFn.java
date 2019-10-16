package com.google.allenday.genomics.core.csv;

import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.gene.UriProvider;
import com.google.allenday.genomics.core.io.FileUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class GeneDataFromMetaDataFn extends DoFn<GeneExampleMetaData, KV<GeneExampleMetaData, List<GeneData>>> {

    private Logger LOG = LoggerFactory.getLogger(GeneDataFromMetaDataFn.class);
    private UriProvider uriProvider;
    private FileUtils fileUtils;

    public GeneDataFromMetaDataFn(UriProvider uriProvider, FileUtils fileUtils) {
        this.uriProvider = uriProvider;
        this.fileUtils = fileUtils;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        GeneExampleMetaData input = c.element();
        LOG.info(String.format("GeneDataFromMetaDataFn %s", input.toString()));

        List<GeneData> geneDataList = uriProvider.provide(input).stream()
                .map(uri -> GeneData.fromBlobUri(uri, fileUtils.getFilenameFromPath(uri))).collect(Collectors.toList());
        c.output(KV.of(input, geneDataList));
    }
}
