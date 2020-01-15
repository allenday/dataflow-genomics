## SRA converter
This script provides a simple way to download SRA files from [NCBI Sequence Read Archive](https://trace.ncbi.nlm.nih.gov/Traces/sra/) and covert them into [FASTQ](https://en.wikipedia.org/wiki/FASTQ_format) format using [SRA toolkit](https://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?view=toolkit_doc)

Usage:

`python3 sra_converter.py [project_id] [source_filename] [dest_gcs_bucket] [path_to_sra_toolkit_dir]`

Example of source file:

```bash
SRP072226,SRS1357179,SRR3286249,gs://sra-pub-run-3/SRR3286249/SRR3286249.1
SRP072226,SRS1357179,SRR3286250,https://sra-downloadb.be-md.ncbi.nlm.nih.gov/sos1/sra-pub-run-1/SRR3286250/SRR3286250.1
SRP072226,SRS1357179,SRR3286251,gs://sra-pub-run-2/SRR3286251/SRR3286251.1
```