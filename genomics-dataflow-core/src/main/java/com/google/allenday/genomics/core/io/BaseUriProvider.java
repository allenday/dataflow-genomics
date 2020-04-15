package com.google.allenday.genomics.core.io;

import com.google.allenday.genomics.core.model.SampleMetaData;

import java.io.Serializable;

public class BaseUriProvider implements Serializable {

    private String srcBucket;
    private ProviderRule providerRule;

    public BaseUriProvider(String srcBucket, ProviderRule providerRule) {
        this.srcBucket = srcBucket;
        this.providerRule = providerRule;
    }

    public void setProviderRule(ProviderRule providerRule) {
        this.providerRule = providerRule;
    }

    public String provide(SampleMetaData geneSampleMetaData) {
        return providerRule.provideAccordinglyRule(geneSampleMetaData, srcBucket);
    }

    public interface ProviderRule extends Serializable {
        String provideAccordinglyRule(SampleMetaData geneSampleMetaData, String srcBucket);
    }
}