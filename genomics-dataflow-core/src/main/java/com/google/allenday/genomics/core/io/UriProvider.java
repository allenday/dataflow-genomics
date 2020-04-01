package com.google.allenday.genomics.core.io;

import com.google.allenday.genomics.core.model.SampleMetaData;

import java.io.Serializable;
import java.util.List;

public class UriProvider implements Serializable {

    public static enum FastqExt {
        FASTQ(".fastq"),
        FQ(".fq");

        final String ext;

        FastqExt(String ext) {
            this.ext = ext;
        }

        public static FastqExt defaultExt() {
            return FASTQ;
        }
    }

    private String srcBucket;
    private ProviderRule providerRule;

    public UriProvider(String srcBucket, ProviderRule providerRule) {
        this.srcBucket = srcBucket;
        this.providerRule = providerRule;
    }

    public void setProviderRule(ProviderRule providerRule) {
        this.providerRule = providerRule;
    }

    public List<String> provide(SampleMetaData geneSampleMetaData) {
        return providerRule.provideAccordinglyRule(geneSampleMetaData, srcBucket);
    }

    public interface ProviderRule extends Serializable {
        List<String> provideAccordinglyRule(SampleMetaData geneSampleMetaData, String srcBucket);
    }
}