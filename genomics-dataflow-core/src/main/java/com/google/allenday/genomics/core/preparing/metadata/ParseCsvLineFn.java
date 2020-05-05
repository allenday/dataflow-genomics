package com.google.allenday.genomics.core.preparing.metadata;

import com.google.allenday.genomics.core.model.SampleRunMetaData;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ParseCsvLineFn extends DoFn<String, SampleRunMetaData> {

    private Logger LOG = LoggerFactory.getLogger(ParseCsvLineFn.class);
    private SampleRunMetaData.Parser csvParser;

    public ParseCsvLineFn(SampleRunMetaData.Parser csvParser) {
        this.csvParser = csvParser;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String input = c.element();
        LOG.info(String.format("Parse %s", input));
        try {
            SampleRunMetaData geneSampleRunMetaData = SampleRunMetaData.fromCsvLine(csvParser, input);
            c.output(geneSampleRunMetaData);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
