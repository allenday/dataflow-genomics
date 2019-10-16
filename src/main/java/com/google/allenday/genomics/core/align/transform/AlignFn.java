package com.google.allenday.genomics.core.align.transform;

import com.google.allenday.genomics.core.align.AlignService;
import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.TransformIoHandler;
import com.google.allenday.genomics.core.reference.ReferencesProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class AlignFn extends DoFn<KV<GeneExampleMetaData, List<GeneData>>, KV<GeneExampleMetaData, GeneData>> {

    private Logger LOG = LoggerFactory.getLogger(AlignFn.class);
    private GCSService gcsService;

    private AlignService alignService;
    private ReferencesProvider referencesProvider;
    private List<String> referenceNames;
    private TransformIoHandler transformIoHandler;
    private FileUtils fileUtils;

    public AlignFn(AlignService alignService,
                   ReferencesProvider referencesProvider,
                   List<String> referenceNames,
                   TransformIoHandler transformIoHandler,
                   FileUtils fileUtils) {
        this.alignService = alignService;
        this.referencesProvider = referencesProvider;
        this.referenceNames = referenceNames;
        this.transformIoHandler = transformIoHandler;
        this.fileUtils = fileUtils;
    }

    @Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
        alignService.setupMinimap2();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of align with input: %s", c.element().toString()));

        GeneExampleMetaData geneExampleMetaData = c.element().getKey();
        List<GeneData> geneDataList = c.element().getValue();

        if (geneExampleMetaData == null || geneDataList.size() == 0) {
            LOG.error("Data error");
            LOG.error("geneExampleMetaData: " + geneExampleMetaData);
            LOG.error("geneDataList.size(): " + geneDataList.size());
            return;
        }
        try {
            String workingDir = fileUtils.makeUniqueDirWithTimestampAndSuffix(geneExampleMetaData.getRunId());
            try {
                List<String> srcFilesPaths = geneDataList.stream()
                        .map(geneData -> transformIoHandler.handleInputAsLocalFile(gcsService, geneData, workingDir))
                        .collect(Collectors.toList());

                for (String referenceName : referenceNames) {
                    String referencePath = referencesProvider.findReference(gcsService, referenceName);

                    //TODO temp
                    String alignedSamName = workingDir + "_" + geneExampleMetaData.getRunId() + ".sam";
                    String alignedSamPath = workingDir + alignedSamName;
                    boolean exists = TransformIoHandler.tryToFindInPrevious(gcsService, alignedSamName, alignedSamPath, "", "");

                    if (!exists) {
                        alignedSamPath = alignService.alignFastq(referencePath, srcFilesPaths, workingDir, geneExampleMetaData.getRunId(), referenceName);
                    }
                    c.output(KV.of(geneExampleMetaData, transformIoHandler.handleFileOutput(gcsService, alignedSamPath, referenceName)));
                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            } finally {
                fileUtils.deleteDir(workingDir);
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }


}
