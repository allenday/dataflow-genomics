package com.google.allenday.genomics.core.main.gene;

import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class GeneExampleMetaDataTests {

    @Test
    public void testGeneExampleMetaDataParser() throws IOException {
        String projectName = "projectName";
        String projectId = "projectId";
        String bioSample = "bio,Sample";
        String sraSample = "sra,Sample";
        String runId = "runId";
        String isPaired = "PAIRED";

        String csvLine = String.format("%s,%s,\"%s\",\"%s\",%s,,%s", projectName, projectId, bioSample, sraSample, runId, isPaired);
        GeneExampleMetaData.Parser parser = GeneExampleMetaData.Parser.withDefaultSchema();
        GeneExampleMetaData geneExampleMetaData = parser.parse(csvLine);

        Assert.assertEquals(new GeneExampleMetaData(projectName, projectId, bioSample, sraSample, runId, true, csvLine), geneExampleMetaData);
    }
}
