package com.google.allenday.genomics.core.model;


import org.apache.commons.validator.routines.IntegerValidator;

public class SraParser extends GeneExampleMetaData.Parser {

    @Override
    public GeneExampleMetaData processParts(String[] csvLineParts, String csvLine) throws CsvParseException {
        try {
            GeneExampleMetaData geneExampleMetaData = new GeneExampleMetaData(csvLineParts[11], csvLineParts[10], csvLineParts[6],
                    csvLine)
                    .setSampleName(csvLineParts[12]);
            geneExampleMetaData.setAvgSpotLen(IntegerValidator.getInstance().validate(csvLineParts[0]));
            geneExampleMetaData.setBioSample(csvLineParts[1]);
            geneExampleMetaData.setDatastoreProvider(csvLineParts[2]);
            geneExampleMetaData.setDatastoreRegion(csvLineParts[3]);
            geneExampleMetaData.setExperiment(csvLineParts[4]);
            geneExampleMetaData.setInsertSize(IntegerValidator.getInstance().validate(csvLineParts[5]));
            geneExampleMetaData.setLibraryName(csvLineParts[7]);
            geneExampleMetaData.setNumBases(IntegerValidator.getInstance().validate(csvLineParts[8]));
            geneExampleMetaData.setNumBytes(IntegerValidator.getInstance().validate(csvLineParts[9]));
            geneExampleMetaData.setAssayType(csvLineParts[13]);
            geneExampleMetaData.setBioProject(csvLineParts[14]);
            geneExampleMetaData.setCenterName(csvLineParts[15]);
            geneExampleMetaData.setConsent(csvLineParts[16]);
            geneExampleMetaData.setDatastoreFiletype(csvLineParts[17]);
            geneExampleMetaData.setInstrument(csvLineParts[18]);
            geneExampleMetaData.setLibrarySelection(csvLineParts[19]);
            geneExampleMetaData.setLibrarySource(csvLineParts[20]);
            geneExampleMetaData.setLoadDate(csvLineParts[21]);
            geneExampleMetaData.setOrganism(csvLineParts[22]);
            geneExampleMetaData.setPlatform(csvLineParts[23]);
            geneExampleMetaData.setReleaseDate(csvLineParts[24]);
            geneExampleMetaData.setSraStudy(csvLineParts[25]);
            return geneExampleMetaData;
        } catch (Exception e) {
            throw new CsvParseException(csvLine);
        }
    }
}
