package com.google.allenday.genomics.core.processing.align;

import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.reference.ReferenceDatabaseSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.util.List;

public class AlignTransform extends PTransform<PCollection<KV<SampleMetaData, List<FileWrapper>>>,
        PCollection<KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>>> {

    private AlignFn alignFn;
    private AddReferenceDataSourceFn addReferenceDataSourceFn;

    public AlignTransform(@Nullable String name, AlignFn alignFn, AddReferenceDataSourceFn addReferenceDataSourceFn) {
        super(name);
        this.alignFn = alignFn;
        this.addReferenceDataSourceFn = addReferenceDataSourceFn;
    }

    public PCollection<KV<SampleMetaData, KV<ReferenceDatabaseSource, FileWrapper>>> expand(
            PCollection<KV<SampleMetaData, List<FileWrapper>>> input) {
        return input.apply("Add all references", ParDo.of(addReferenceDataSourceFn))
                .apply("Align reads", ParDo.of(alignFn));
    }
}
