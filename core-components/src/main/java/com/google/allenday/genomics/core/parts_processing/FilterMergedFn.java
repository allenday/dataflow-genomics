package com.google.allenday.genomics.core.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.ReadGroupMetaData;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FilterMergedFn extends DoFn<KV<ReadGroupMetaData, Iterable<SampleMetaData>>, KV<KV<ReadGroupMetaData, ReferenceDatabase>, List<FileWrapper>>> {

    private Logger LOG = LoggerFactory.getLogger(FilterMergedFn.class);

    private GCSService gcsService;

    private FileUtils fileUtils;
    private List<String> references;
    private String stagedBucket;

    private String mergedFilePattern;
    private String sortedFilePattern;

    public FilterMergedFn(FileUtils fileUtils, List<String> references,
                          String stagedBucket, String mergedFilePattern, String sortedFilePattern) {
        this.fileUtils = fileUtils;
        this.references = references;
        this.stagedBucket = stagedBucket;
        this.sortedFilePattern = sortedFilePattern;
        this.mergedFilePattern = mergedFilePattern;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<ReadGroupMetaData, Iterable<SampleMetaData>> input = c.element();
        ReadGroupMetaData geneReadGroupMetaData = input.getKey();

        Iterable<SampleMetaData> geneSampleMetaDataIterable = input.getValue();

        for (String ref : references) {
            BlobId blobIdMerge = BlobId.of(stagedBucket, String.format(mergedFilePattern, geneReadGroupMetaData.getSraSample(), ref));
            boolean mergeExists = gcsService.isExists(blobIdMerge);
            if (!mergeExists) {
                boolean redyToMerge = true;
                List<FileWrapper> fileWrappers = new ArrayList<>();
                for (SampleMetaData geneSampleMetaData : geneSampleMetaDataIterable) {
                    BlobId blobIdSort = BlobId.of(stagedBucket, String.format(sortedFilePattern, geneSampleMetaData.getRunId(), ref));
                    boolean sortExists = gcsService.isExists(blobIdSort);
                    String uriFromBlob = gcsService.getUriFromBlob(blobIdSort);
                    fileWrappers.add(FileWrapper.fromBlobUri(uriFromBlob,
                            new FileUtils().getFilenameFromPath(uriFromBlob)));
                    if (!sortExists) {
                        redyToMerge = false;
                        LOG.info(String.format("Not ready to merge: %s", geneReadGroupMetaData.getSraSample()));
                    }
                }
                if (redyToMerge) {
                    c.output(KV.of(KV.of(geneReadGroupMetaData, new ReferenceDatabase(ref, new ArrayList<>())), fileWrappers));
                }
            }
        }
    }
}
