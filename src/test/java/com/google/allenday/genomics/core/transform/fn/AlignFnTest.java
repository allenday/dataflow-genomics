package com.google.allenday.genomics.core.transform.fn;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.io.IoHandler;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.javatuples.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

/**
 * Tests full pipeline lifecycle in DataflowRunner mode
 */
public class AlignFnTest {


    //    private final static String PROJECT = "cannabis-3k";
//
//    private final static String SRC_BUCKET = "cannabis-3k";
//    private final static String TEST_EXAMPLE_PROJECT = "SRP092005";
//    private final static String TEST_EXAMPLE_SRA = "SRS1760342";
//    private final static String TEST_EXAMPLE_RUN = "SRR4451179";
//    private final static String RESULT_BUCKET = "cannabis-3k-results";
//    private final static String REFERENCE_DIR = "reference/";
//
//    private final static String ALIGN_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_aligned_bam/";
//    private final static String SORT_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_sorted_bam/";
//    private final static String MERGE_RESULT_GCS_DIR_PATH_PATTERN = "testing/cannabis_processing_output/%s/result_merged_bam/";
//
//    private final static String REFERENCE_NAME = "AGQN03";
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void testEndToEndPipeline() {
        CmdExecutor cmdExecutorMock = Mockito.mock(CmdExecutor.class, Mockito.withSettings().serializable());
        WorkerSetupService workerSetupServiceMock = Mockito.mock(WorkerSetupService.class);
        IoHandler ioHandlerMock = Mockito.mock(IoHandler.class);

        Mockito.when(cmdExecutorMock.executeCommand(anyString())).thenReturn(Pair.with(true, 0));

        List<GeneData> geneDataList = new ArrayList<GeneData>(){{
            add(GeneData.fromByteArrayContent("123".getBytes(), "input.fastq"));
            add(GeneData.fromByteArrayContent("123".getBytes(), "input.fastq"));
        }};

        GeneExampleMetaData geneExampleMetaData = new GeneExampleMetaData("test_project","test_project_id","test_bio_sample","tes_sra_sample","test_run", "");

        /*testPipeline
                .apply(Create.<KV<GeneExampleMetaData, Iterable<GeneData>>>of(KV.of(geneExampleMetaData, geneDataList)))
                .apply(ParDo.of(new AlignFn(cmdExecutorMock, workerSetupServiceMock, Collections.singletonList("reference_1"), ioHandlerMock)));*/

        PipelineResult pipelineResult = testPipeline.run();
        pipelineResult.waitUntilFinish();
    }
}