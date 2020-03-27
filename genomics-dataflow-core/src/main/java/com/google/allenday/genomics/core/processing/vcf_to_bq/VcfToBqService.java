package com.google.allenday.genomics.core.processing.vcf_to_bq;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.processing.lifesciences.LifeSciencesService;
import com.google.allenday.genomics.core.utils.ResourceProvider;
import com.google.cloud.storage.BlobId;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class VcfToBqService implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(VcfToBqService.class);

    private final static String VCF_TO_BQ_SCRIPT_PATH = "/opt/gcp_variant_transforms/bin/vcf_to_bq";
    private final static String DEFAULT_VCF_TO_BQ_LOCATION_REGION = "us-central1";
    private final static String DEFAULT_VCF_TO_BQ_DATAFLOW_RUNNER = "DataflowRunner";
    private final static String VCF_TO_BQ_JOB_NAME_PREFIX = "vcf-to-bq-";


    private final static String VCF_TO_BQ_IMAGE_URI = "gcr.io/cloud-lifesciences/gcp-variant-transforms";
    private final static String VCF_TO_BQ_MACHINE_TYPE = "n1-standard-1";


    private Boolean append = true;
    private String region;

    public enum DeepVariantArguments {
        PROJECT("project"),
        INPUT_PATTERN("input_pattern"),
        OUTPUT_TABLE("output_table"),
        TEMP_LOCATION("temp_location"),
        JOB_NAME("job_name"),
        RUNNER("runner"),
        APPEND("append"),
        REGION("region");

        private final String argName;

        DeepVariantArguments(String argName) {
            this.argName = argName;
        }

        public String getArgForCommand() {
            return "--" + argName;
        }
    }

    private LifeSciencesService lifeSciencesService;
    private String vcfToBqTablePathPattern;
    private String tempAndLogsGcsDir;
    private String tempAndLogsBucket;
    private String jobStartTime;

    public VcfToBqService(LifeSciencesService lifeSciencesService,
                          String vcfToBqTablePathPattern,
                          String tempAndLogsBucket,
                          String tempAndLogsGcsDir,
                          String jobStartTime) {
        this.lifeSciencesService = lifeSciencesService;
        this.vcfToBqTablePathPattern = vcfToBqTablePathPattern;
        this.tempAndLogsBucket = tempAndLogsBucket;
        this.tempAndLogsGcsDir = tempAndLogsGcsDir;
        this.jobStartTime = jobStartTime;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public Pair<Boolean, String> convertVcfFileToBq(GCSService gcsService,
                                                    ResourceProvider resourceProvider,
                                                    FileUtils fileUtils,
                                                    String referenceName,
                                                    String vcfFileUri) {

        String[] parts = vcfFileUri.split("/");
        String suffix = parts[parts.length - 1].contains("*") ? parts[parts.length - 2] : fileUtils.changeFileExtension(parts[parts.length - 1], "");

        String jobTag = jobStartTime + "_" + referenceName + "_" + suffix;
        String outputPath = gcsService.getUriFromBlob(BlobId.of(tempAndLogsBucket, tempAndLogsGcsDir)) + jobTag + "/";

        String outputTable = String.format(vcfToBqTablePathPattern, referenceName);
        List<String> actionCommands = buildCommand(resourceProvider, VCF_TO_BQ_JOB_NAME_PREFIX + jobTag.toLowerCase().replace("_", "-"),
                vcfFileUri, outputTable, outputPath);

        String selectedRegion = Optional.ofNullable(region).orElse(DEFAULT_VCF_TO_BQ_LOCATION_REGION);
        Pair<Boolean, String> operationResult = lifeSciencesService.runLifesciencesPipelineWithLogging(actionCommands,
                VCF_TO_BQ_IMAGE_URI, outputPath, selectedRegion, VCF_TO_BQ_MACHINE_TYPE,
                resourceProvider.getProjectNumber(), jobTag);

        return operationResult;
    }


    private List<String> buildCommand(ResourceProvider resourceProvider,
                                      String jobName,
                                      String inputPattern,
                                      String outputTable,
                                      String tempLocation) {
        Map<DeepVariantArguments, String> args = new HashMap<>();

        args.put(DeepVariantArguments.PROJECT, resourceProvider.getProjectId());
        args.put(DeepVariantArguments.INPUT_PATTERN, inputPattern);
        args.put(DeepVariantArguments.OUTPUT_TABLE, outputTable);
        args.put(DeepVariantArguments.TEMP_LOCATION, tempLocation);
        args.put(DeepVariantArguments.JOB_NAME, jobName);
        args.put(DeepVariantArguments.RUNNER, DEFAULT_VCF_TO_BQ_DATAFLOW_RUNNER);
        args.put(DeepVariantArguments.REGION, region != null ? region : DEFAULT_VCF_TO_BQ_LOCATION_REGION);
        if (append) {
            args.put(DeepVariantArguments.APPEND, "");
        }

        List<String> command = new ArrayList<>();
        command.add(VCF_TO_BQ_SCRIPT_PATH);
        for (Map.Entry<DeepVariantArguments, String> entry : args.entrySet()) {
            command.add(entry.getKey().getArgForCommand());
            command.add(entry.getValue());
        }
        LOG.info(String.format("VcfToBq command generated: \n%s\n", String.join("\n", command)));
        return command;
    }

}