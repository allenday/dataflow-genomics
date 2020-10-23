package com.google.allenday.genomics.core.preparing.runfile.uriprovider;

import com.google.allenday.genomics.core.model.SampleRunMetaData;

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

    public String provide(SampleRunMetaData geneSampleRunMetaData) {
        return providerRule.provideAccordinglyRule(geneSampleRunMetaData, srcBucket);
    }

    public interface ProviderRule extends Serializable {
        String provideAccordinglyRule(SampleRunMetaData geneSampleRunMetaData, String srcBucket);
    }
}