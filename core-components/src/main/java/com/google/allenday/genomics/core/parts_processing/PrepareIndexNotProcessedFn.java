package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraSampleId;
import com.google.allenday.genomics.core.model.SraSampleIdReferencePair;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;

public class PrepareIndexNotProcessedFn extends DoFn<KV<SraSampleId, Iterable<SampleMetaData>>,
        KV<SraSampleIdReferencePair, KV<ReferenceDatabaseSource, FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareIndexNotProcessedFn.class);

    private GCSService gcsService;

    private FileUtils fileUtils;
    private ValueProvider<List<String>> referencesVP;
    private StagingPathsBulder stagingPathsBulder;
    private ValueProvider<String> allReferencesDirGcsUri;

    public PrepareIndexNotProcessedFn(FileUtils fileUtils, ValueProvider<List<String>> referencesVP,
                                      StagingPathsBulder stagingPathsBulder, ValueProvider<String> allReferencesDirGcsUri) {
        this.fileUtils = fileUtils;
        this.referencesVP = referencesVP;
        this.stagingPathsBulder = stagingPathsBulder;
        this.allReferencesDirGcsUri = allReferencesDirGcsUri;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SraSampleId, Iterable<SampleMetaData>> input = c.element();

        @Nonnull
        SraSampleId sraSampleId = input.getKey();

        List<String> references = referencesVP.get();
        for (String ref : references) {
            BlobId blobIdMerge = stagingPathsBulder.buildMergedBlobId(sraSampleId.getValue(), ref);
            boolean mergeExists = gcsService.isExists(blobIdMerge);
            BlobId blobIdIndex = stagingPathsBulder.buildIndexBlobId(sraSampleId.getValue(), ref);
            boolean indexExists = gcsService.isExists(blobIdIndex);
            if (mergeExists && !indexExists) {
                String uriFromBlob = gcsService.getUriFromBlob(blobIdMerge);
                ReferenceDatabaseSource referenceDatabaseSource =
                        new ReferenceDatabaseSource.ByNameAndUriSchema(ref, allReferencesDirGcsUri.get());
                c.output(KV.of(new SraSampleIdReferencePair(sraSampleId, referenceDatabaseSource.getName()),
                        KV.of(referenceDatabaseSource, FileWrapper.fromBlobUri(uriFromBlob, new FileUtils().getFilenameFromPath(uriFromBlob)))));
            }
        }
    }
}
