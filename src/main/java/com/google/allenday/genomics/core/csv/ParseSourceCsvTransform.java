package com.google.allenday.genomics.core.csv;

import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.gene.UriProvider;
import com.google.allenday.genomics.core.io.FileUtils;
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
        PCollection<KV<GeneExampleMetaData, List<GeneData>>>> {

    private String csvGcsUri;
    private GeneExampleMetaData.Parser csvParser;
    private UriProvider uriProvider;
    private FileUtils fileUtils;

    private List<String> sraSamplesToFilter;
    private PTransform<PCollection<KV<GeneExampleMetaData, List<GeneData>>>,
            PCollection<KV<GeneExampleMetaData, List<GeneData>>>> preparingTransforms;

    public void setSraSamplesToFilter(List<String> sraSamplesToFilter) {
        this.sraSamplesToFilter = sraSamplesToFilter;
    }

    public void setPreparingTransforms(PTransform<PCollection<KV<GeneExampleMetaData, List<GeneData>>>,
            PCollection<KV<GeneExampleMetaData, List<GeneData>>>> preparingTransforms) {
        this.preparingTransforms = preparingTransforms;
    }

    public ParseSourceCsvTransform(String csvGcsUri, GeneExampleMetaData.Parser csvParser, UriProvider uriProvider, FileUtils fileUtils) {
        this.csvGcsUri = csvGcsUri;
        this.csvParser = csvParser;
        this.uriProvider = uriProvider;
        this.fileUtils = fileUtils;
    }

    public ParseSourceCsvTransform(@Nullable String name, String csvGcsUri, GeneExampleMetaData.Parser csvParser, UriProvider uriProvider, FileUtils fileUtils) {
        super(name);
        this.csvGcsUri = csvGcsUri;
        this.csvParser = csvParser;
        this.uriProvider = uriProvider;
        this.fileUtils = fileUtils;
    }

    @Override
    public PCollection<KV<GeneExampleMetaData, List<GeneData>>> expand(PBegin pBegin) {
        PCollection<String> csvLines = pBegin.apply("Read data from CSV", TextIO.read().from(csvGcsUri));
        if (sraSamplesToFilter != null && sraSamplesToFilter.size() > 0) {
            csvLines
                    .apply("Filter lines", Filter.by(name -> sraSamplesToFilter.stream().anyMatch(name::contains)));
        }
        PCollection<KV<GeneExampleMetaData, List<GeneData>>> readyToAlign = csvLines
                .apply("Parse CSV line", ParDo.of(new ParseCsvLineFn(csvParser)))
                .apply("Gene data from meta data", ParDo.of(new GeneDataFromMetaDataFn(uriProvider, fileUtils)));
        if (preparingTransforms != null) {
            readyToAlign = readyToAlign.apply(preparingTransforms);
        }
        return readyToAlign;
    }
}
