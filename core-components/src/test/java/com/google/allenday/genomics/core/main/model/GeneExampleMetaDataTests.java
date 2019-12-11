package com.google.allenday.genomics.core.main.model;

import com.google.allenday.genomics.core.model.GeneExampleMetaData;
import com.google.allenday.genomics.core.model.SraParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


public class GeneExampleMetaDataTests {

    @Test
    public void testGeneExampleMetaDataParser() throws IOException {
        String runId = "SRR306863";
        String libraryLayout = "SINGLE";
        String sampleName = "CSA_S_AC";

        int avgSpotLen = 35;
        String bioSample = "SAMN00632260";
        String datastoreProvider = "gs s3 sra-sos";
        String datastoreRegion = "gs.US s3.us-east-1sra-sos.be-md sra-sos.st-va";
        String experiment = "SRX082008";
        int insertSize = 0;

        String libraryName = "CSA_AC";
        int numBases = 877;
        int numBytes = 490;

        String sraSample = "SRS214102";
        String assayType = "RNA-Seq";
        String bioProject = "PRJNA80055";
        String centerName = "MICHIGAN STATE UNIVERSITY";
        String consent = "public";
        String datastoreFiletype = "sra";
        String instrument = "Illumina Genome Analyzer IIx";
        String librarySelection = "cDNA";
        String librarySource = "TRANSCRIPTOMIC";
        String loadDate = "2014-05-30";
        String organism = "Cannabis sativa";
        String platform = "ILLUMINA";
        String releaseDate = "2011-09-30";
        String sraStudy = "SRP006678";

        String csvLine = "35,SAMN00632260,gs s3 sra-sos,gs.US s3.us-east-1sra-sos.be-md sra-sos.st-va,SRX082008,0,SINGLE,CSA_AC,877,490,SRR306863,SRS214102,CSA_S_AC,RNA-Seq,PRJNA80055,MICHIGAN STATE UNIVERSITY,public,sra,Illumina Genome Analyzer IIx,cDNA,TRANSCRIPTOMIC,2014-05-30,Cannabis sativa,ILLUMINA,2011-09-30,SRP006678";
        GeneExampleMetaData.Parser parser = new SraParser(GeneExampleMetaData.Parser.Separation.COMMA);
        GeneExampleMetaData geneExampleMetaData = parser.parse(csvLine);

        GeneExampleMetaData geneExampleMetaDataExpected = new GeneExampleMetaData(sraSample, runId, libraryLayout, csvLine);

        geneExampleMetaDataExpected.setSampleName(sampleName);
        geneExampleMetaDataExpected.setAvgSpotLen(avgSpotLen);
        geneExampleMetaDataExpected.setBioSample(bioSample);
        geneExampleMetaDataExpected.setDatastoreProvider(datastoreProvider);
        geneExampleMetaDataExpected.setDatastoreRegion(datastoreRegion);
        geneExampleMetaDataExpected.setExperiment(experiment);
        geneExampleMetaDataExpected.setInsertSize(insertSize);
        geneExampleMetaDataExpected.setLibraryName(libraryName);
        geneExampleMetaDataExpected.setNumBases(numBases);
        geneExampleMetaDataExpected.setNumBytes(numBytes);
        geneExampleMetaDataExpected.setAssayType(assayType);
        geneExampleMetaDataExpected.setBioProject(bioProject);
        geneExampleMetaDataExpected.setCenterName(centerName);
        geneExampleMetaDataExpected.setConsent(consent);
        geneExampleMetaDataExpected.setDatastoreFiletype(datastoreFiletype);
        geneExampleMetaDataExpected.setInstrument(instrument);
        geneExampleMetaDataExpected.setLibrarySelection(librarySelection);
        geneExampleMetaDataExpected.setLibrarySource(librarySource);
        geneExampleMetaDataExpected.setLoadDate(loadDate);
        geneExampleMetaDataExpected.setOrganism(organism);
        geneExampleMetaDataExpected.setPlatform(platform);
        geneExampleMetaDataExpected.setReleaseDate(releaseDate);
        geneExampleMetaDataExpected.setSraStudy(sraStudy);

        Assert.assertEquals(geneExampleMetaDataExpected, geneExampleMetaData);
    }
}
