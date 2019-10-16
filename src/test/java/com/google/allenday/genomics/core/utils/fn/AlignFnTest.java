package com.google.allenday.genomics.core.utils.fn;

import com.google.allenday.genomics.core.align.transform.AlignFn;
import com.google.allenday.genomics.core.align.AlignService;
import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;


public class AlignFnTest implements Serializable {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void testAlignFn() throws IOException {
        String resultName = "result_1.sam";

        AlignService alignServiceMock = Mockito.mock(AlignService.class, Mockito.withSettings().serializable());
        ReferencesProvider referencesProviderv = Mockito.mock(ReferencesProvider.class, Mockito.withSettings().serializable());
        FileUtils fileUtilsMock = Mockito.mock(FileUtils.class, Mockito.withSettings().serializable());
        TransformIoHandler transformIoHandlerMock = Mockito.mock(TransformIoHandler.class, Mockito.withSettings().serializable());

        List<String> referenceList = new ArrayList<String>() {
            {
                add("reference_1");
                add("reference_2");
            }
        };

        Mockito.when(alignServiceMock.alignFastq(anyString(), any(), anyString(), anyString(), anyString())).thenReturn(resultName);
        for (String reference : referenceList) {
            Mockito.when(transformIoHandlerMock.handleFileOutput(any(), Mockito.eq(resultName), Mockito.eq(reference)))
                    .thenReturn(GeneData.fromBlobUri("result_uri", resultName)
                            .withReferenceName(reference));
        }

        List<GeneData> geneDataList = new ArrayList<GeneData>() {{
            add(GeneData.fromByteArrayContent("1".getBytes(), "input_1.fastq"));
            add(GeneData.fromByteArrayContent("2".getBytes(), "input_2.fastq"));
        }};

        GeneExampleMetaData geneExampleMetaData = new GeneExampleMetaData("test_project", "test_project_id",
                "test_bio_sample", "tes_sra_sample", "test_run", false, "");

        PCollection<KV<GeneExampleMetaData, GeneData>> alignedData = testPipeline
                .apply(Create.<KV<GeneExampleMetaData, List<GeneData>>>of(KV.of(geneExampleMetaData, geneDataList)))
                .apply(ParDo.of(new AlignFn(alignServiceMock, referencesProviderv, referenceList, transformIoHandlerMock, fileUtilsMock)));

        PAssert.that(alignedData)
                .satisfies(new SimpleFunction<Iterable<KV<GeneExampleMetaData, GeneData>>, Void>() {
                    @Override
                    public Void apply(Iterable<KV<GeneExampleMetaData, GeneData>> input) {
                        List<KV<GeneExampleMetaData, GeneData>> outputList = StreamSupport.stream(input.spliterator(), false).collect(Collectors.toList());

                        Assert.assertEquals("Output list equals to reference list", outputList.size(), referenceList.size());
                        for (String reference : referenceList) {
                            Assert.assertTrue("Output contains reference",
                                    outputList.stream().map(el -> el.getValue().getReferenceName()).anyMatch(ref -> ref.equals(reference)));

                        }
                        return null;
                    }

                });
        PipelineResult pipelineResult = testPipeline.run();
        pipelineResult.waitUntilFinish();
    }
}