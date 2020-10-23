package com.google.allenday.genomics.core.pipeline.batch.partsprocessing;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PrepareSortNotProcessedFn extends DoFn<KV<SampleRunMetaData, List<FileWrapper>>,
        KV<SampleRunMetaData, KV<ReferenceDatabaseSource, FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareSortNotProcessedFn.class);

    private GcsService gcsService;

    private FileUtils fileUtils;
    private List<String> references;

    private StagingPathsBulder stagingPathsBulder;
    private String allReferencesDirGcsUri;


    public PrepareSortNotProcessedFn(FileUtils fileUtils,
                                     List<String> references,
                                     StagingPathsBulder stagingPathsBulder,
                                     String allReferencesDirGcsUri) {
        this.fileUtils = fileUtils;
        this.references = references;
        this.stagingPathsBulder = stagingPathsBulder;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
    }

    @Setup
    public void setUp() {
        gcsService = GcsService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SampleRunMetaData, List<FileWrapper>> input = c.element();
        SampleRunMetaData geneSampleRunMetaData = input.getKey();


        for (String ref : references) {
            BlobId blobIdSort = stagingPathsBulder.buildSortedBlobId(geneSampleRunMetaData.getRunId(), ref);
            BlobId blobIdAlign = stagingPathsBulder.buildAlignedBlobId(geneSampleRunMetaData.getRunId(), ref);

            boolean existsAligned = gcsService.isExists(blobIdAlign);
            boolean existsSorted = gcsService.isExists(blobIdSort);
            if (existsAligned && !existsSorted) {
                String uriFromBlob = gcsService.getUriFromBlob(blobIdAlign);

                LOG.info(String.format("Pass to %s: %s", "SORTED", geneSampleRunMetaData.getRunId()));
                c.output(KV.of(geneSampleRunMetaData,
                        KV.of(new ReferenceDatabaseSource.ByNameAndUriSchema(ref, allReferencesDirGcsUri),
                                FileWrapper.fromBlobUri(uriFromBlob, new FileUtils().getFilenameFromPath(uriFromBlob)))));
            }
        }
    }
}
