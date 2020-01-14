package com.google.allenday.nanostream.cannabis.io;

import com.google.allenday.genomics.core.model.SampleMetaData;

public class CannabisCsvParser extends SampleMetaData.Parser {

    public CannabisCsvParser() {
        super(Separation.COMMA);
    }

    @Override
    public SampleMetaData processParts(String[] csvLineParts, String csvLine) throws CsvParseException {
        try {
            SampleMetaData geneSampleMetaData = new SampleMetaData(csvLineParts[3], csvLineParts[4], csvLineParts[6], csvLine);
            geneSampleMetaData.setCenterName(csvLineParts[0]);
            geneSampleMetaData.setSraStudy(csvLineParts[1]);
            geneSampleMetaData.setBioProject(csvLineParts[2]);
            return geneSampleMetaData;
        } catch (RuntimeException e) {
            throw new CsvParseException(csvLine);
        }
    }
}
