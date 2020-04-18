package com.google.allenday.genomics.core.io;

public class DefaultBaseUriProvider extends BaseUriProvider {

    private final static String DEFAULT_SRC_DIR_URI_PATTERN = "gs://%s/fastq/%s/%s/";

    public DefaultBaseUriProvider(String srcBucket, ProviderRule providerRule) {
        super(srcBucket, providerRule);
    }

    public static DefaultBaseUriProvider withDefaultProviderRule(String srcBucket) {
        return new DefaultBaseUriProvider(srcBucket, (ProviderRule) (geneSampleMetaData, bucket) -> {
            String uriPrefix = String.format(DEFAULT_SRC_DIR_URI_PATTERN, bucket, geneSampleMetaData.getSraStudy(),
                    geneSampleMetaData.getSraSample());
            return uriPrefix + geneSampleMetaData.getRunId();
        });
    }
}