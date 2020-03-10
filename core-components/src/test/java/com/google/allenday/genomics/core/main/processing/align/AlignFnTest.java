package com.google.allenday.genomics.core.main.processing.align;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.processing.align.AlignFn;
import com.google.allenday.genomics.core.processing.align.AlignService;
import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
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

        List<ReferenceDatabaseSource> referenceList = new ArrayList<ReferenceDatabaseSource>() {
            {
                add(new ReferenceDatabaseSource.ByNameAndUriSchema("reference_1", "reference_1"));
                add(new ReferenceDatabaseSource.ByNameAndUriSchema("reference_2", "reference_2"));
            }
        };
        for (ReferenceDatabaseSource reference : referenceList) {
            Mockito.when(referencesProvider.getReferenceDbWithDownload(any(), eq(reference)))
                    .thenReturn(new ReferenceDatabase.Builder(reference.getName(), reference.getName()).build());
        }
        Mockito.when(alignServiceMock.alignFastq(anyString(), any(), anyString(), anyString(), anyString(), anyString(), any())).thenReturn(resultName);
        Mockito.when(transformIoHandlerMock.handleFileOutput(any(), Mockito.eq(resultName)))
                .thenReturn(FileWrapper.fromBlobUri("result_uri", resultName));

        List<FileWrapper> fileWrapperList = new ArrayList<FileWrapper>() {{
            add(FileWrapper.fromByteArrayContent("1".getBytes(), "input_1.fastq"));
            add(FileWrapper.fromByteArrayContent("2".getBytes(), "input_2.fastq"));
        }};

        SampleMetaData geneSampleMetaData = new SampleMetaData("tes_sra_sample", "test_run",
                "Single", AlignService.Instrument.ILLUMINA.name(), "");

        PCollection<KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>> alignedData = testPipeline
                .apply(Create.of(KV.of(geneSampleMetaData, KV.of(referenceList, fileWrapperList))))
                .apply(ParDo.of(new AlignFn(alignServiceMock, referencesProvider, transformIoHandlerMock, fileUtilsMock)));

        PAssert.that(alignedData)
                .satisfies(new SimpleFunction<Iterable<KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>>, Void>() {
                    @Override
                    public Void apply(Iterable<KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>> input) {
                        List<KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>> outputList = StreamSupport.stream(input.spliterator(), false)
                                .collect(Collectors.toList());

                        Assert.assertEquals("Output list equals to reference list", referenceList.size(), outputList.size());
                        for (ReferenceDatabaseSource reference : referenceList) {
                            Assert.assertTrue("Output contains reference",
                                    outputList.stream().map(el -> el.getValue().getKey()).anyMatch(ref -> ref.equals(reference)));

                        }
                        return null;
                    }

                });
        PipelineResult pipelineResult = testPipeline.run();
        pipelineResult.waitUntilFinish();
    }
}