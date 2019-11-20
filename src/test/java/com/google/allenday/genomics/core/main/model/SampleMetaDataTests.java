package com.google.allenday.genomics.core.main.model;

import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.model.SraParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


public class SampleMetaDataTests {

    @Test
    public void testSampleMetaDataParser() throws IOException {
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
        SampleMetaData.Parser parser = new SraParser();
        SampleMetaData sampleMetaData = parser.parse(csvLine);

        SampleMetaData sampleMetaDataExpected = new SampleMetaData(sraSample, runId, libraryLayout, csvLine);

        sampleMetaDataExpected.setSampleName(sampleName);
        sampleMetaDataExpected.setAvgSpotLen(avgSpotLen);
        sampleMetaDataExpected.setBioSample(bioSample);
        sampleMetaDataExpected.setDatastoreProvider(datastoreProvider);
        sampleMetaDataExpected.setDatastoreRegion(datastoreRegion);
        sampleMetaDataExpected.setExperiment(experiment);
        sampleMetaDataExpected.setInsertSize(insertSize);
        sampleMetaDataExpected.setLibraryName(libraryName);
        sampleMetaDataExpected.setNumBases(numBases);
        sampleMetaDataExpected.setNumBytes(numBytes);
        sampleMetaDataExpected.setAssayType(assayType);
        sampleMetaDataExpected.setBioProject(bioProject);
        sampleMetaDataExpected.setCenterName(centerName);
        sampleMetaDataExpected.setConsent(consent);
        sampleMetaDataExpected.setDatastoreFiletype(datastoreFiletype);
        sampleMetaDataExpected.setInstrument(instrument);
        sampleMetaDataExpected.setLibrarySelection(librarySelection);
        sampleMetaDataExpected.setLibrarySource(librarySource);
        sampleMetaDataExpected.setLoadDate(loadDate);
        sampleMetaDataExpected.setOrganism(organism);
        sampleMetaDataExpected.setPlatform(platform);
        sampleMetaDataExpected.setReleaseDate(releaseDate);
        sampleMetaDataExpected.setSraStudy(sraStudy);

        Assert.assertEquals(sampleMetaDataExpected, sampleMetaData);
    }
}
