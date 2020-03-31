[![Maven Central](https://img.shields.io/maven-central/v/com.google.allenday/genomics-dataflow-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.google.allenday%22%20AND%20a:%22genomics-dataflow-core%22)

# Dataflow Genomics Core Components
Ready-to-use components for implementation [Google Cloud Dataflow](https://cloud.google.com/dataflow) pipelines to solve genomics processing tasks

## Overview
Here you can find a wide list of components for building genomics data processing pipelines 
based on [Apache Beam](https://beam.apache.org/) unified programming model and runnable 
with [Google Cloud Dataflow](https://cloud.google.com/dataflow). Current package includes tools for:
- Building Batch and Streaming processing transformation graphs of genomics data   
- Working with [SRA](https://www.ncbi.nlm.nih.gov/sra/) metadata annotations
- Manipulations with [FASTQ](https://en.wikipedia.org/wiki/FASTQ_format) files
- [Sequence alignment](https://en.wikipedia.org/wiki/Sequence_alignment)
- Different [SAM/BAM](https://samtools.github.io/hts-specs/SAMv1.pdf) data manipulations (Sorting, Merging, etc.) 
- [Variant Calling](https://www.ebi.ac.uk/training/online/course/human-genetic-variation-i-introduction-2019/variant-identification-and-analysis)
- Variant Calling results (VCF) export
- Wotking with [FASTA](https://en.wikipedia.org/wiki/FASTA_format) genome references

### Prerequisites
- [Java Development Kit](https://www.oracle.com/technetwork/java/javase/downloads/index.html) (JDK) __version 8__
- [Apache Maven](https://maven.apache.org/download.cgi)

### Structure
The repository contains two Maven modules:
- [genomics-dataflow-core](genomics-dataflow-core) - module with Dataflow Genomics Core Components Java source code
- [giab-example](giab-example) - module with demo project, that shows an example of usage of [genomics-dataflow-core](genomics-dataflow-core)

### High-level components

There are several high-level classes, that could be used as the main building blocks for your pipeline. Here are some of them:

- [ParseSourceCsvTransform](genomics-dataflow-core/src/main/java/com/google/allenday/genomics/core/csv/ParseSourceCsvTransform.java) - provides queue of input data transformation. 
It includes reading input CSV file ([example](docs/sra_reads_annotations_example.csv)), parsing, filtering, check for anomalies in metadata. Return ready to use key-value pair of [SampleMetaData](genomics-dataflow-core/src/main/java/com/google/allenday/genomics/core/model/SampleMetaData.java) and list of [FileWrapper](genomics-dataflow-core/src/main/java/com/google/allenday/genomics/core/model/FileWrapper.java)
- [SplitFastqIntoBatches](genomics-dataflow-core/src/main/java/com/google/allenday/genomics/core/processing/SplitFastqIntoBatches.java) - provides FASTQ splitting mechanism to increase parallelism and balance load between workers
- [AlignAndPostProcessTransform](genomics-dataflow-core/src/main/java/com/google/allenday/genomics/core/processing/AlignAndPostProcessTransform.java) - contains queue of genomics transformation namely [Sequence alignment](https://en.wikipedia.org/wiki/Sequence_alignment) (FASTQ->SAM), converting to binary format (SAM->BAM), sorting FASTQ and merging FASTQ in scope of single sample
- [DeepVariantFn](genomics-dataflow-core/src/main/java/com/google/allenday/genomics/core/processing/dv/DeepVariantFn.java) - Apache Beam DoFn function, that provides [Variant Calling](https://www.ebi.ac.uk/training/online/course/human-genetic-variation-i-introduction-2019/variant-identification-and-analysis) logic. Currently supported [Deep Variant](https://github.com/google/deepvariant) variant caller pipeline from Google.
- [VcfToBqFn](genomics-dataflow-core/src/main/java/com/google/allenday/genomics/core/processing/vcf_to_bq/VcfToBqFn.java) - Apache Beam DoFn function, that exports Variant Calling results (VCF) into the [BigQuery](https://cloud.google.com/bigquery) table. Uses vcf-to-bigquery transform from [GCP Variant Transforms
](https://github.com/googlegenomics/gcp-variant-transforms)

### Sequence aligning
By default, [minimap2](https://github.com/lh3/minimap2) aligner is used for Sequence aligning stage. Optionally you can use [BWA] aligner by setting `--aligner=bwa` option.

Also, you can add a custom aligner by extending [AlignService](genomics-dataflow-core/src/main/java/com/google/allenday/genomics/core/processing/align/AlignService.java) class

## Usage
This repository contains an [example](giab-example) of usage of Dataflow Genomics Core Components library, that provides a demo pipeline with batch processing of the [NA12878](https://www.coriell.org/0/Sections/Search/Sample_Detail.aspx?Ref=NA12878&product=DNA) sample from [Genome in a Bottle](https://www.nist.gov/programs-projects/genome-bottle).
 
### Already used by
[Nanostream Dataflow](https://github.com/allenday/nanostream-dataflow) - a scalable, reliable, and cost effective end-to-end pipeline for fast DNA sequence analysis using Dataflow on Google Cloud

[GCP-PopGen Processing Pipeline](https://github.com/allenday/gcp-popgen) - a repository, that contains a number of Apache Beam pipeline configurations for processing different populations of genomes (e.g. Homo Sapiens, Rice, Cannabis)
### Testing
Repository contains unit test that covers all main components and one end-to-end integration test.
For integration testing you have to configure `TEST_BUCKET` environment variable.
