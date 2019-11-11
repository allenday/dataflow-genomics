package com.google.allenday.genomics.core.main.processing.align;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.GeneExampleMetaData;
import com.google.allenday.genomics.core.model.ReferenceDatabase;
import com.google.allenday.genomics.core.processing.align.AlignFn;
import com.google.allenday.genomics.core.processing.align.AlignService;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.mockito.Matchers.*;


public class AlignFnTest implements Serializable {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void testAlignFn() throws IOException {
        String resultName = "result_1.sam";

        AlignService alignServiceMock = Mockito.mock(AlignService.class, Mockito.withSettings().serializable());
        ReferencesProvider referencesProvider = Mockito.mock(ReferencesProvider.class, Mockito.withSettings().serializable());
        FileUtils fileUtilsMock = Mockito.mock(FileUtils.class, Mockito.withSettings().serializable());
        TransformIoHandler transformIoHandlerMock = Mockito.mock(TransformIoHandler.class, Mockito.withSettings().serializable());

        List<String> referenceList = new ArrayList<String>() {
            {
                add("reference_1");
                add("reference_2");
            }
        };

        Mockito.when(alignServiceMock.alignFastq(anyString(), any(), anyString(), anyString(), anyString(), anyString())).thenReturn(resultName);
        for (String reference : referenceList) {
            Mockito.when(referencesProvider.findReference(any(), eq(reference))).thenReturn(Pair.with(new ReferenceDatabase(reference, Collections.emptyList()),
                    "ref_path"));
        }
        Mockito.when(transformIoHandlerMock.handleFileOutput(any(), Mockito.eq(resultName)))
                .thenReturn(FileWrapper.fromBlobUri("result_uri", resultName));

        List<FileWrapper> fileWrapperList = new ArrayList<FileWrapper>() {{
            add(FileWrapper.fromByteArrayContent("1".getBytes(), "input_1.fastq"));
            add(FileWrapper.fromByteArrayContent("2".getBytes(), "input_2.fastq"));
        }};

        GeneExampleMetaData geneExampleMetaData = new GeneExampleMetaData("tes_sra_sample", "test_run", "Single", "");

        PCollection<KV<KV<GeneExampleMetaData, ReferenceDatabase>, FileWrapper>> alignedData = testPipeline
                .apply(Create.<KV<GeneExampleMetaData, List<FileWrapper>>>of(KV.of(geneExampleMetaData, fileWrapperList)))
                .apply(ParDo.of(new AlignFn(alignServiceMock, referencesProvider, referenceList, transformIoHandlerMock, fileUtilsMock)));

        PAssert.that(alignedData)
                .satisfies(new SimpleFunction<Iterable<KV<KV<GeneExampleMetaData, ReferenceDatabase>, FileWrapper>>, Void>() {
                    @Override
                    public Void apply(Iterable<KV<KV<GeneExampleMetaData, ReferenceDatabase>, FileWrapper>> input) {
                        List<KV<KV<GeneExampleMetaData, ReferenceDatabase>, FileWrapper>> outputList = StreamSupport.stream(input.spliterator(), false).collect(Collectors.toList());

                        Assert.assertEquals("Output list equals to reference list", referenceList.size(), outputList.size());
                        for (String reference : referenceList) {
                            Assert.assertTrue("Output contains reference",
                                    outputList.stream().map(el -> el.getKey().getValue().getDbName()).anyMatch(ref -> ref.equals(reference)));

                        }
                        return null;
                    }

                });
        PipelineResult pipelineResult = testPipeline.run();
        pipelineResult.waitUntilFinish();
    }
}