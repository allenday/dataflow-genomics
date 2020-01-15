package com.google.allenday.genomics.core.csv;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.UriProvider;
import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.util.List;

public class ParseSourceCsvTransform extends PTransform<PBegin,
        PCollection<KV<SampleMetaData, List<FileWrapper>>>> {

    private String csvGcsUri;
    private SampleMetaData.Parser csvParser;
    private UriProvider uriProvider;
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

    public ParseSourceCsvTransform(String csvGcsUri, SampleMetaData.Parser csvParser, UriProvider uriProvider, FileUtils fileUtils) {
        this.csvGcsUri = csvGcsUri;
        this.csvParser = csvParser;
        this.uriProvider = uriProvider;
        this.fileUtils = fileUtils;
    }

    public ParseSourceCsvTransform(@Nullable String name, String csvGcsUri, SampleMetaData.Parser csvParser, UriProvider uriProvider, FileUtils fileUtils) {
        super(name);
        this.csvGcsUri = csvGcsUri;
        this.csvParser = csvParser;
        this.uriProvider = uriProvider;
        this.fileUtils = fileUtils;
    }

    @Override
    public PCollection<KV<SampleMetaData, List<FileWrapper>>> expand(PBegin pBegin) {
        PCollection<String> csvLines = pBegin.apply("Read data from CSV", TextIO.read().from(csvGcsUri));
        if (sraSamplesToFilter != null && sraSamplesToFilter.size() > 0) {
            csvLines = csvLines
                    .apply("Filter lines", Filter.by(name -> sraSamplesToFilter.stream().anyMatch(name::contains)));
        }
        if (sraSamplesToSkip != null && sraSamplesToSkip.size() > 0) {
            csvLines = csvLines
                    .apply("Filter lines", Filter.by(name -> sraSamplesToSkip.stream().noneMatch(name::contains)));
        }
        PCollection<KV<SampleMetaData, List<FileWrapper>>> readyToAlign = csvLines
                .apply("Parse CSV line", ParDo.of(new ParseCsvLineFn(csvParser)))
                .apply("Gene data from meta data", ParDo.of(new GeneDataFromMetaDataFn(uriProvider, fileUtils)));
        if (preparingTransforms != null) {
            readyToAlign = readyToAlign.apply(preparingTransforms);
        }
        return readyToAlign;
    }
}
