package com.google.allenday.nanostream.rice.io;

import com.google.allenday.genomics.core.io.UriProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RiceUriProvider extends UriProvider {

    public RiceUriProvider(String srcBucket, ProviderRule providerRule) {
        super(srcBucket, providerRule);
    }

    public static RiceUriProvider withDefaultProviderRule(String srcBucket) {
        return new RiceUriProvider(srcBucket, (ProviderRule) (geneSampleMetaData, bucket) -> {
            String uriPrefix = String.format("gs://%s/PRJEB6180/fastq/%s/%s/", bucket, geneSampleMetaData.getSraStudy(),
                    geneSampleMetaData.getSraSample());
            String fileNameForward = geneSampleMetaData.getRunId() + "_1.fastq";
            List<String> urisList =
                    new ArrayList<>(Collections.singletonList(uriPrefix + fileNameForward));
            if (geneSampleMetaData.isPaired()) {
                String fileNameBack = geneSampleMetaData.getRunId() + "_2.fastq";
                urisList.add(uriPrefix + fileNameBack);
            }
            return urisList;
        });
    }
}