package com.google.allenday.genomics.core.main.model;

import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.metadata.shema.SraCsvSchema;
import org.checkerframework.checker.units.qual.A;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


public class SampleRunMetaDataTests {

    @Test
    public void testGeneSampleMetaDataParser() throws IOException {
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
        String dataSourceType = "SRA";
        String dataSourceUri = null;

        String csvLine = "35,SAMN00632260,gs s3 sra-sos,gs.US s3.us-east-1sra-sos.be-md sra-sos.st-va,SRX082008,0," +
                "SINGLE,CSA_AC,877,490,SRR306863,SRS214102,CSA_S_AC,RNA-Seq,PRJNA80055,MICHIGAN STATE UNIVERSITY,public," +
                "sra,Illumina Genome Analyzer IIx,cDNA,TRANSCRIPTOMIC,2014-05-30,Cannabis sativa,ILLUMINA," +
                "2011-09-30,SRP006678,SRA";
        SampleRunMetaData.Parser parser = new SampleRunMetaData.Parser(new SraCsvSchema(),
                SampleRunMetaData.Parser.Separation.COMMA);
        SampleRunMetaData sampleRunMetaData = parser.parse(csvLine);

        SampleRunMetaData sampleRunMetaDataExpected = new SampleRunMetaData(sraSample, runId, libraryLayout, platform,
                SampleRunMetaData.DataSource.create(dataSourceType, dataSourceUri));
        sampleRunMetaDataExpected.setRawMetaData(csvLine);
        sampleRunMetaDataExpected.setSraStudy(sraStudy);

        SampleRunMetaData.AdditionalMetaData additionalMetaData = new SampleRunMetaData.AdditionalMetaData();
        additionalMetaData.setSampleName(sampleName);
        additionalMetaData.setAvgSpotLen(avgSpotLen);
        additionalMetaData.setBioSample(bioSample);
        additionalMetaData.setDatastoreProvider(datastoreProvider);
        additionalMetaData.setDatastoreRegion(datastoreRegion);
        additionalMetaData.setExperiment(experiment);
        additionalMetaData.setInsertSize(insertSize);
        additionalMetaData.setLibraryName(libraryName);
        additionalMetaData.setNumBases(numBases);
        additionalMetaData.setNumBytes(numBytes);
        additionalMetaData.setAssayType(assayType);
        additionalMetaData.setBioProject(bioProject);
        additionalMetaData.setCenterName(centerName);
        additionalMetaData.setConsent(consent);
        additionalMetaData.setDatastoreFiletype(datastoreFiletype);
        additionalMetaData.setInstrument(instrument);
        additionalMetaData.setLibrarySelection(librarySelection);
        additionalMetaData.setLibrarySource(librarySource);
        additionalMetaData.setLoadDate(loadDate);
        additionalMetaData.setOrganism(organism);
        additionalMetaData.setReleaseDate(releaseDate);

        sampleRunMetaDataExpected.setAdditionalMetaData(additionalMetaData);

        Assert.assertEquals(sampleRunMetaDataExpected, sampleRunMetaData);
    }
}
