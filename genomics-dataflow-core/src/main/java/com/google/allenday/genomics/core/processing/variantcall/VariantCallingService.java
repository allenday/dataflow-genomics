package com.google.allenday.genomics.core.processing.variantcall;

import com.google.allenday.genomics.core.reference.ReferenceDatabase;
import com.google.allenday.genomics.core.gcp.ResourceProvider;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public abstract class VariantCallingService implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(VariantCallingService.class);

    public final static String DEEP_VARIANT_RESULT_EXTENSION = ".vcf";

    public abstract void setup();

    public abstract Triplet<String, Boolean, String> processSampleWithVariantCaller(ResourceProvider resourceProvider,
                                                                                    String outDirGcsUri,
                                                                                    String outFileNameWithoutExt,
                                                                                    String bamUri,
                                                                                    String baiUri,
                                                                                    String region,
                                                                                    ReferenceDatabase referenceDatabase,
                                                                                    String readGroupName);
}
