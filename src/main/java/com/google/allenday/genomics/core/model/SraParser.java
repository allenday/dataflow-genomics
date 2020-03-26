package com.google.allenday.genomics.core.model;


import org.apache.commons.validator.routines.IntegerValidator;

public class SraParser extends SampleMetaData.Parser {

    public SraParser() {
    }

    public SraParser(Separation separation) {
        super(separation);
    }

    @Override
    public SampleMetaData processParts(String[] csvLineParts, String csvLine) throws CsvParseException {
        try {
            SampleMetaData geneSampleMetaData = new SampleMetaData(csvLineParts[11], csvLineParts[10], csvLineParts[6],
                    csvLineParts[23],
                    csvLine);
            geneSampleMetaData.setSampleName(csvLineParts[12]);
            geneSampleMetaData.setAvgSpotLen(IntegerValidator.getInstance().validate(csvLineParts[0]));
            geneSampleMetaData.setBioSample(csvLineParts[1]);
            geneSampleMetaData.setDatastoreProvider(csvLineParts[2]);
            geneSampleMetaData.setDatastoreRegion(csvLineParts[3]);
            geneSampleMetaData.setExperiment(csvLineParts[4]);
            geneSampleMetaData.setInsertSize(IntegerValidator.getInstance().validate(csvLineParts[5]));
            geneSampleMetaData.setLibraryName(csvLineParts[7]);
            geneSampleMetaData.setNumBases(IntegerValidator.getInstance().validate(csvLineParts[8]));
            geneSampleMetaData.setNumBytes(IntegerValidator.getInstance().validate(csvLineParts[9]));
            geneSampleMetaData.setAssayType(csvLineParts[13]);
            geneSampleMetaData.setBioProject(csvLineParts[14]);
            geneSampleMetaData.setCenterName(csvLineParts[15]);
            geneSampleMetaData.setConsent(csvLineParts[16]);
            geneSampleMetaData.setDatastoreFiletype(csvLineParts[17]);
            geneSampleMetaData.setInstrument(csvLineParts[18]);
            geneSampleMetaData.setLibrarySelection(csvLineParts[19]);
            geneSampleMetaData.setLibrarySource(csvLineParts[20]);
            geneSampleMetaData.setLoadDate(csvLineParts[21]);
            geneSampleMetaData.setOrganism(csvLineParts[22]);
            geneSampleMetaData.setReleaseDate(csvLineParts[24]);
            geneSampleMetaData.setSraStudy(csvLineParts[25]);
            return geneSampleMetaData;
        } catch (Exception e) {
            e.printStackTrace();
            throw new CsvParseException(csvLine);
        }
    }
}
