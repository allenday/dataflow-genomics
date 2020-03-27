package com.google.allenday.genomics.core.csv;

import com.google.allenday.genomics.core.model.SampleMetaData;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseCsvLineFn extends DoFn<String, SampleMetaData> {

    private Logger LOG = LoggerFactory.getLogger(ParseCsvLineFn.class);
    private SampleMetaData.Parser csvParser;

    public ParseCsvLineFn(SampleMetaData.Parser csvParser) {
        this.csvParser = csvParser;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String input = c.element();
        LOG.info(String.format("Parse %s", input));
        try {
            SampleMetaData geneSampleMetaData = SampleMetaData.fromCsvLine(csvParser, input);
            c.output(geneSampleMetaData);
        } catch (SampleMetaData.Parser.CsvParseException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
