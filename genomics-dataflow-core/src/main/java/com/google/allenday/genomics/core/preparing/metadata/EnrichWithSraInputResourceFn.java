package com.google.allenday.genomics.core.preparing.metadata;

import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.runfile.SraInputResource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichWithSraInputResourceFn extends DoFn<SampleRunMetaData, KV<SampleRunMetaData, SraInputResource>> {

    private Logger LOG = LoggerFactory.getLogger(EnrichWithSraInputResourceFn.class);


    @ProcessElement
    public void processElement(ProcessContext c) {
        SampleRunMetaData input = c.element();
        LOG.info(input.toString());

        if (input.getDataSource().getType() == SampleRunMetaData.DataSource.Type.SRA) {
            c.output(KV.of(input, new SraInputResource(input.getRunId())));
        }
    }
}
