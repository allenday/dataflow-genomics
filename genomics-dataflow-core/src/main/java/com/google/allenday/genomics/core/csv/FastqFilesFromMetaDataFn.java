package com.google.allenday.genomics.core.csv;

import com.google.allenday.genomics.core.io.BaseUriProvider;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class FastqFilesFromMetaDataFn extends DoFn<SampleMetaData, KV<SampleMetaData, List<FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(FastqFilesFromMetaDataFn.class);
    private BaseUriProvider uriProvider;
    private FileUtils fileUtils;
    private GCSService gcsService;

    public FastqFilesFromMetaDataFn(BaseUriProvider uriProvider, FileUtils fileUtils) {
        this.uriProvider = uriProvider;
        this.fileUtils = fileUtils;
    }

    /**
     * Only fot test purpose
     */
    public FastqFilesFromMetaDataFn setGcsService(GCSService gcsService) {
        this.gcsService = gcsService;
        return this;
    }

    @Setup
    public void setup() {
        if (gcsService == null) {
            gcsService = GCSService.initialize(fileUtils);
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        SampleMetaData input = c.element();
        LOG.info(String.format("FastqFilesFromMetaDataFn %s", input.toString()));

        String baseUri = uriProvider.provide(input);
        BlobId baseUriBlobId = gcsService.getBlobIdFromUri(baseUri);
        List<FileWrapper> fileWrapperList = gcsService.getBlobsIdsWithPrefixList(baseUriBlobId.getBucket(), baseUriBlobId.getName())
                .stream()
                .map(gcsService::getUriFromBlob)
                .sorted(new Comparator<String>() {
                    @Override
                    public int compare(String uri1, String uri2) {
                        return uri1.replace(baseUri, "").compareTo(uri2.replace(baseUri, ""));
                    }
                })
                .map(uri -> FileWrapper.fromBlobUri(uri, fileUtils.getFilenameFromPath(uri)))
                .collect(Collectors.toList());
        c.output(KV.of(input, fileWrapperList));
    }
}
