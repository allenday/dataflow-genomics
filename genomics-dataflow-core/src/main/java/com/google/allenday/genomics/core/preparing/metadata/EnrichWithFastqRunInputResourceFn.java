package com.google.allenday.genomics.core.preparing.metadata;

import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.runfile.FastqInputResource;
import com.google.allenday.genomics.core.preparing.runfile.GcsFastqInputResource;
import com.google.allenday.genomics.core.preparing.runfile.UrlFastqInputResource;
import com.google.allenday.genomics.core.preparing.runfile.uriprovider.BaseUriProvider;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class EnrichWithFastqRunInputResourceFn extends DoFn<SampleRunMetaData, KV<SampleRunMetaData, List<FastqInputResource>>> {

    private Logger LOG = LoggerFactory.getLogger(EnrichWithFastqRunInputResourceFn.class);
    private BaseUriProvider uriProvider;
    private FileUtils fileUtils;
    private GcsService gcsService;

    public EnrichWithFastqRunInputResourceFn(BaseUriProvider uriProvider, FileUtils fileUtils) {
        this.uriProvider = uriProvider;
        this.fileUtils = fileUtils;
    }

    /**
     * Only fot test purpose
     */
    public EnrichWithFastqRunInputResourceFn setGcsService(GcsService gcsService) {
        this.gcsService = gcsService;
        return this;
    }

    @Setup
    public void setup() {
        if (gcsService == null) {
            gcsService = GcsService.initialize(fileUtils);
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        SampleRunMetaData input = c.element();
        LOG.info(input.toString());

        List<FastqInputResource> fastqInputResources = null;

        if (input.getDataSource().getType() == SampleRunMetaData.DataSource.Type.URL) {
            fastqInputResources = input.getDataSource().getUris().stream()
                    .map(UrlFastqInputResource::new).collect(Collectors.toList());
        } else if (input.getDataSource().getType() == SampleRunMetaData.DataSource.Type.GCS) {
            fastqInputResources = input.getDataSource().getUris().stream()
                    .map(uri -> gcsService.getBlobIdFromUri(uri)).map(GcsFastqInputResource::new)
                    .collect(Collectors.toList());
        } else if (input.getDataSource().getType() == SampleRunMetaData.DataSource.Type.GCS_URI_PROVIDER) {
            String baseUri = uriProvider.provide(input);
            BlobId baseUriBlobId = gcsService.getBlobIdFromUri(baseUri);
            fastqInputResources = gcsService.getBlobsIdsWithPrefixList(baseUriBlobId.getBucket(), baseUriBlobId.getName())
                    .stream()
                    .map(GcsFastqInputResource::new)
                    .collect(Collectors.toList());
        }
        if (fastqInputResources != null) {
            fastqInputResources = fastqInputResources.stream()
                    .sorted(Comparator.comparing(FastqInputResource::getName))
                    .collect(Collectors.toList());
            c.output(KV.of(input, fastqInputResources));
        }
    }
}
