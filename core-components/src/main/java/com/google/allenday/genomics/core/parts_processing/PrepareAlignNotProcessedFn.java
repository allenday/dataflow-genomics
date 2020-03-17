package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class PrepareAlignNotProcessedFn extends DoFn<KV<SampleMetaData, List<FileWrapper>>,
        KV<SampleMetaData, KV<List<ReferenceDatabaseSource>, List<FileWrapper>>>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareAlignNotProcessedFn.class);

    private GCSService gcsService;

    private FileUtils fileUtils;
    private ValueProvider<List<String>> referencesVP;

    private StagingPathsBulder stagingPathsBulder;
    private ValueProvider<String> allReferencesDirGcsUri;


    public PrepareAlignNotProcessedFn(FileUtils fileUtils,
                                      ValueProvider<List<String>> referencesVP,
                                      StagingPathsBulder stagingPathsBulder,
                                      ValueProvider<String> allReferencesDirGcsUri) {
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
        KV<SampleMetaData, List<FileWrapper>> input = c.element();
        SampleMetaData geneSampleMetaData = input.getKey();

        List<String> references = referencesVP.get();

        for (String ref : references) {

            BlobId blobIdAlign = stagingPathsBulder.buildAlignedBlobId(geneSampleMetaData.getRunId(), ref);
            boolean exists = gcsService.isExists(blobIdAlign);
            if (!exists) {
                LOG.info(String.format("Pass to %s: %s", "ALIGN", geneSampleMetaData.getRunId()));
                c.output(KV.of(geneSampleMetaData,
                        KV.of(Collections.singletonList(new ReferenceDatabaseSource.ByNameAndUriSchema(ref, allReferencesDirGcsUri.get())), input.getValue())));
            }
        }
    }
}
