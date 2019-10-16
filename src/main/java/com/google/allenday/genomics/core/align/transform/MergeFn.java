package com.google.allenday.genomics.core.align.transform;

import com.google.allenday.genomics.core.align.SamBamManipulationService;
import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneReadGroupMetaData;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MergeFn extends DoFn<KV<KV<GeneReadGroupMetaData, String>, List<GeneData>>, KV<GeneReadGroupMetaData, GeneData>> {


    private Logger LOG = LoggerFactory.getLogger(MergeFn.class);
    private GCSService gcsService;

    private TransformIoHandler transformIoHandler;
    private SamBamManipulationService samBamManipulationService;
    private FileUtils fileUtils;

    public MergeFn(TransformIoHandler transformIoHandler, SamBamManipulationService samBamManipulationService, FileUtils fileUtils) {
        this.transformIoHandler = transformIoHandler;
        this.samBamManipulationService = samBamManipulationService;
        this.fileUtils = fileUtils;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    private boolean isNothingToMerge(List<GeneData> geneDataList) {
        return geneDataList.size() < 2;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Merge of sort with input: %s", c.element().toString()));

        KV<GeneReadGroupMetaData, String> geneReadGroupMetaDataAndReference = c.element().getKey();

        if (geneReadGroupMetaDataAndReference == null) {
            LOG.error("Data error");
            LOG.error("geneReadGroupMetaDataAndReference: " + geneReadGroupMetaDataAndReference);
            return;
        }
        GeneReadGroupMetaData geneReadGroupMetaData = geneReadGroupMetaDataAndReference.getKey();
        String reference = geneReadGroupMetaDataAndReference.getValue();

        List<GeneData> geneDataList = c.element().getValue();

        if (geneReadGroupMetaData == null || geneDataList.size() == 0) {
            LOG.error("Data error");
            LOG.error("geneReadGroupMetaData: " + geneReadGroupMetaDataAndReference);
            LOG.error("geneDataList.size(): " + geneDataList.size());
            return;
        }

        try {
            String workDir = fileUtils.makeUniqueDirWithTimestampAndSuffix(geneReadGroupMetaData.getSraSample());
            try {
                if (isNothingToMerge(geneDataList)) {
                    geneDataList.stream().findFirst().ifPresent(inputGeneData -> {
                                GeneData geneData = transformIoHandler.handleInputAndCopyToGcs(inputGeneData, gcsService,
                                        samBamManipulationService.generateMergedFileName(geneReadGroupMetaData.getSraSample(), reference),
                                        reference, workDir);
                                c.output(KV.of(geneReadGroupMetaData, geneData));
                            }
                    );
                } else {
                    List<String> localBamPaths = geneDataList.stream()
                            .map(geneData -> transformIoHandler.handleInputAsLocalFile(gcsService, geneData, workDir))
                            .collect(Collectors.toList());

                    String mergedFileName = samBamManipulationService.mergeBamFiles(localBamPaths, workDir, geneReadGroupMetaData.getSraSample(), reference);
                    GeneData geneData = transformIoHandler.saveFileToGcsOutput(gcsService, mergedFileName, reference);
                    c.output(KV.of(geneReadGroupMetaData, geneData));

                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            } finally {
                fileUtils.deleteDir(workDir);
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
