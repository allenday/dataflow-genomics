package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.List;
import java.util.stream.Collectors;

public abstract class AddReferenceDataSourceFn extends DoFn<KV<SampleMetaData, List<FileWrapper>>,
        KV<SampleMetaData, KV<List<ReferenceDatabaseSource>, List<FileWrapper>>>> {

    public static class FromNameAndDirPath extends AddReferenceDataSourceFn {

        private ValueProvider<String> allReferencesDirGcsUriVP;
        private ValueProvider<List<String>> referenceNamesVP;

        public FromNameAndDirPath(ValueProvider<String> allReferencesDirGcsUriVP, ValueProvider<List<String>> referenceNamesVP) {
            this.allReferencesDirGcsUriVP = allReferencesDirGcsUriVP;
            this.referenceNamesVP = referenceNamesVP;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String allReferencesDirGcsUri = allReferencesDirGcsUriVP.get();
            List<String> referenceNames = referenceNamesVP.get();
            List<ReferenceDatabaseSource> refDBSources = referenceNames.stream()
                    .map(refName -> new ReferenceDatabaseSource.ByNameAndUriSchema(refName, allReferencesDirGcsUri))
                    .collect(Collectors.toList());

            c.output(KV.of(c.element().getKey(), KV.of(refDBSources, c.element().getValue())));

        }
    }

    public static class Explicitly extends AddReferenceDataSourceFn {

        private ValueProvider<String> refDataJsonStringVP;

        public Explicitly(ValueProvider<String> refDataJsonStringVP) {
            this.refDataJsonStringVP = refDataJsonStringVP;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String refDataJsonString = refDataJsonStringVP.get();
            List<ReferenceDatabaseSource> referenceDatabaseSources = ReferenceDatabaseSource.Explicit.fromRefDataJsonString(refDataJsonString);

            c.output(KV.of(c.element().getKey(), KV.of(referenceDatabaseSources, c.element().getValue())));
        }
    }
}