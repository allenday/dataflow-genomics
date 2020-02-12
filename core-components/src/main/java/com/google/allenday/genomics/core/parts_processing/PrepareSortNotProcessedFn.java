package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PrepareSortNotProcessedFn extends DoFn<KV<SampleMetaData, List<FileWrapper>>,
        KV<KV<SampleMetaData, ReferenceDatabase>, FileWrapper>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareSortNotProcessedFn.class);

    private GCSService gcsService;

    private FileUtils fileUtils;
    private List<String> references;

    private StagingPathsBulder stagingPathsBulder;


    public PrepareSortNotProcessedFn(FileUtils fileUtils, List<String> references, StagingPathsBulder stagingPathsBulder) {
        this.fileUtils = fileUtils;
        this.references = references;
        this.stagingPathsBulder = stagingPathsBulder;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SampleMetaData, List<FileWrapper>> input = c.element();
        SampleMetaData geneSampleMetaData = input.getKey();

        for (String ref : references) {
            BlobId blobIdSort = stagingPathsBulder.buildSortedBlobId(geneSampleMetaData.getRunId(), ref);
            BlobId blobIdAlign = stagingPathsBulder.buildAlignedBlobId(geneSampleMetaData.getRunId(), ref);

            boolean existsAligned = gcsService.isExists(blobIdAlign);
            boolean existsSorted = gcsService.isExists(blobIdSort);
            if (existsAligned && !existsSorted) {
                String uriFromBlob = gcsService.getUriFromBlob(blobIdAlign);

                LOG.info(String.format("Pass to %s: %s", "SORTED", geneSampleMetaData.getRunId()));
                c.output(KV.of(KV.of(geneSampleMetaData, new ReferenceDatabase(ref, new ArrayList<>())), FileWrapper.fromBlobUri(
                        uriFromBlob,
                        new FileUtils().getFilenameFromPath(uriFromBlob))));
            }
        }
    }
}
