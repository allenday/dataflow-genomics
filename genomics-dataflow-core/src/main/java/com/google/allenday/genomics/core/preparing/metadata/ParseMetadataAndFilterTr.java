package com.google.allenday.genomics.core.preparing.metadata;

import com.google.allenday.genomics.core.model.SampleRunMetaData;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class ParseMetadataAndFilterTr extends PTransform<PBegin,
        PCollection<SampleRunMetaData>> {

    private String csvGcsUri;
    private SampleRunMetaData.Parser csvParser;
    private List<SampleRunMetaData.DataSource.Type> dataSourceTypesToFilter;

    private List<String> sraSamplesToFilter;
    private List<String> sraSamplesToSkip;


    public ParseMetadataAndFilterTr(String csvGcsUri,
                                    SampleRunMetaData.Parser csvParser,
                                    List<SampleRunMetaData.DataSource.Type> dataSourceTypesToFilter,
                                    List<String> sraSamplesToFilter,
                                    List<String> sraSamplesToSkip) {
        this.csvGcsUri = csvGcsUri;
        this.csvParser = csvParser;
        this.dataSourceTypesToFilter = dataSourceTypesToFilter;
        this.sraSamplesToFilter = sraSamplesToFilter;
        this.sraSamplesToSkip = sraSamplesToSkip;
    }

    @Override
    public PCollection<SampleRunMetaData> expand(PBegin pBegin) {
        PCollection<SampleRunMetaData> metaDataPCollection = pBegin
                .apply("Read data from CSV", TextIO.read().from(csvGcsUri))
                .apply("Parse CSV line", ParDo.of(new ParseCsvLineFn(csvParser)));
        metaDataPCollection = metaDataPCollection
                .apply("Filter lines by DataSource",
                        Filter.by(sample -> dataSourceTypesToFilter.stream()
                                .anyMatch(dataSourceType -> sample.getDataSource().getType().equals(dataSourceType))));
        if (sraSamplesToFilter != null && sraSamplesToFilter.size() > 0) {
            metaDataPCollection = metaDataPCollection
                    .apply("Filter lines", Filter.by(new FilterByList(FilterByList.Mode.FILTER, sraSamplesToFilter)));
        }
        if (sraSamplesToSkip != null && sraSamplesToSkip.size() > 0) {
            metaDataPCollection = metaDataPCollection
                    .apply("Skip lines", Filter.by(new FilterByList(FilterByList.Mode.SKIP, sraSamplesToSkip)));
        }
        return metaDataPCollection;
    }

    public static class FilterByList implements ProcessFunction<SampleRunMetaData, Boolean> {

        Mode mode;
        List<String> sraSamplesToFilter;

        public FilterByList(Mode mode, List<String> sraSamplesToFilter) {
            this.sraSamplesToFilter = sraSamplesToFilter;
            this.mode = mode;
        }

        @Override
        public Boolean apply(SampleRunMetaData input) throws Exception {
            if (mode == Mode.FILTER) {
                return sraSamplesToFilter.stream().anyMatch(sraSampleName -> input.getSraSample().getValue().equals(sraSampleName));
            } else {
                return sraSamplesToFilter.stream().noneMatch(sraSampleName -> input.getSraSample().getValue().equals(sraSampleName));
            }
        }

        public static enum Mode {
            FILTER, SKIP
        }
    }
}
