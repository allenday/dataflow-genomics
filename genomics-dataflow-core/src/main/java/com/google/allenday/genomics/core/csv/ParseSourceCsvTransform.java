package com.google.allenday.genomics.core.csv;

import com.google.allenday.genomics.core.io.BaseUriProvider;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Provides queue of input data transformation.
 * It includes reading input CSV file parsing, filtering, check for anomalies in metadata.
 * Return ready to use key-value pair of {@link SampleMetaData} and list of {@link FileWrapper}
 */
public class ParseSourceCsvTransform extends PTransform<PBegin,
        PCollection<KV<SampleMetaData, List<FileWrapper>>>> {

    private String csvGcsUri;
    private SampleMetaData.Parser csvParser;
    private BaseUriProvider baseUriProvider;
    private FileUtils fileUtils;

    private List<String> sraSamplesToFilter;
    private List<String> sraSamplesToSkip;

    private PTransform<PCollection<KV<SampleMetaData, List<FileWrapper>>>,
            PCollection<KV<SampleMetaData, List<FileWrapper>>>> preparingTransforms;

    public void setSraSamplesToFilter(List<String> sraSamplesToFilter) {
        this.sraSamplesToFilter = sraSamplesToFilter;
    }

    public void setSraSamplesToSkip(List<String> sraSamplesToSkip) {
        this.sraSamplesToSkip = sraSamplesToSkip;
    }

    public void setPreparingTransforms(PTransform<PCollection<KV<SampleMetaData, List<FileWrapper>>>,
            PCollection<KV<SampleMetaData, List<FileWrapper>>>> preparingTransforms) {
        this.preparingTransforms = preparingTransforms;
    }

    public ParseSourceCsvTransform(String csvGcsUri, SampleMetaData.Parser csvParser, BaseUriProvider baseUriProvider, FileUtils fileUtils) {
        this.csvGcsUri = csvGcsUri;
        this.csvParser = csvParser;
        this.baseUriProvider = baseUriProvider;
        this.fileUtils = fileUtils;
    }

    public ParseSourceCsvTransform(@Nullable String name, String csvGcsUri, SampleMetaData.Parser csvParser, BaseUriProvider baseUriProvider, FileUtils fileUtils) {
        super(name);
        this.csvGcsUri = csvGcsUri;
        this.csvParser = csvParser;
        this.baseUriProvider = baseUriProvider;
        this.fileUtils = fileUtils;
    }

    @Override
    public PCollection<KV<SampleMetaData, List<FileWrapper>>> expand(PBegin pBegin) {
        PCollection<SampleMetaData> metaDataPCollection = pBegin
                .apply("Read data from CSV", TextIO.read().from(csvGcsUri))
                .apply("Parse CSV line", ParDo.of(new ParseCsvLineFn(csvParser)));
        if (sraSamplesToFilter != null && sraSamplesToFilter.size() > 0) {
            metaDataPCollection = metaDataPCollection
                    .apply("Filter lines", Filter.by(new FilterByList(FilterByList.Mode.FILTER, sraSamplesToFilter)));
        }
        if (sraSamplesToSkip != null && sraSamplesToSkip.size() > 0) {
            metaDataPCollection = metaDataPCollection
                    .apply("Filter lines", Filter.by(new FilterByList(FilterByList.Mode.SKIP, sraSamplesToSkip)));
        }
        PCollection<KV<SampleMetaData, List<FileWrapper>>> readyToAlign = metaDataPCollection
                .apply("Sample data from metadata", ParDo.of(new FastqFilesFromMetaDataFn(baseUriProvider, fileUtils)));
        if (preparingTransforms != null) {
            readyToAlign = readyToAlign.apply(preparingTransforms);
        }
        return readyToAlign;
    }

    public static class FilterByList implements ProcessFunction<SampleMetaData, Boolean> {

        Mode mode;
        List<String> sraSamplesToFilter;

        public FilterByList(Mode mode, List<String> sraSamplesToFilter) {
            this.sraSamplesToFilter = sraSamplesToFilter;
            this.mode = mode;
        }

        @Override
        public Boolean apply(SampleMetaData input) throws Exception {
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
