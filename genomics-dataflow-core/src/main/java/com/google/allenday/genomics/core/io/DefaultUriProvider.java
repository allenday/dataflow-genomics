package com.google.allenday.genomics.core.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DefaultUriProvider extends UriProvider {

    private final static String DEFAULT_SRC_DIR_URI_PATTERN = "gs://%s/fastq/%s/%s/";

    public DefaultUriProvider(String srcBucket, ProviderRule providerRule) {
        super(srcBucket, providerRule);
    }

    public static DefaultUriProvider withDefaultProviderRule(String srcBucket) {
        return new DefaultUriProvider(srcBucket, (ProviderRule) (geneSampleMetaData, bucket) -> {
            String uriPrefix = String.format(DEFAULT_SRC_DIR_URI_PATTERN, bucket, geneSampleMetaData.getSraStudy(),
                    geneSampleMetaData.getSraSample());
            String fileNameForward = geneSampleMetaData.getRunId() + "_" + 1 + FASTQ_EXTENSION;
            List<String> urisList = new ArrayList<>(Collections.singletonList(uriPrefix + fileNameForward));
            if (geneSampleMetaData.isPaired()) {
                String fileNameBack = geneSampleMetaData.getRunId() + "_" + 2 + FASTQ_EXTENSION;
                urisList.add(uriPrefix + fileNameBack);
            }
            return urisList;
        });
    }
}