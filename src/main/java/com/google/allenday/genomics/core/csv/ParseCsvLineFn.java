package com.google.allenday.genomics.core.csv;

import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseCsvLineFn extends DoFn<String, GeneExampleMetaData> {

    private Logger LOG = LoggerFactory.getLogger(ParseCsvLineFn.class);
    private GeneExampleMetaData.Parser csvParser;

    public ParseCsvLineFn(GeneExampleMetaData.Parser csvParser) {
        this.csvParser = csvParser;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String input = c.element();
        LOG.info(String.format("Parse %s", input));
        GeneExampleMetaData geneExampleMetaData = GeneExampleMetaData.fromCsvLine(csvParser, input);
        c.output(geneExampleMetaData);
    }
}
