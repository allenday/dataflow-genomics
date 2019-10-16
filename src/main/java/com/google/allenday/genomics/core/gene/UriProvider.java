package com.google.allenday.genomics.core.gene;

import java.io.Serializable;
import java.util.List;

public class UriProvider implements Serializable {

    private String srcBucket;
    private ProviderRule providerRule;

    public UriProvider(String srcBucket, ProviderRule providerRule) {
        this.srcBucket = srcBucket;
        this.providerRule = providerRule;
    }

    public void setProviderRule(ProviderRule providerRule) {
        this.providerRule = providerRule;
    }

    public List<String> provide(GeneExampleMetaData geneExampleMetaData) {
        return providerRule.provideAccordinglyRule(geneExampleMetaData, srcBucket);
    }

    public interface ProviderRule extends Serializable{
        List<String> provideAccordinglyRule(GeneExampleMetaData geneExampleMetaData, String srcBucket);
    }
}