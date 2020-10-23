## Example of usage with GIAB sample

Current directory contains an example of usage of [genomics-dataflow-core](../genomics-dataflow-core) library. 
As an example, here presented the batch processing of the [Genome in a Bottle](https://www.nist.gov/programs-projects/genome-bottle) sample. As a source, we choose FASTQ data [NA12878](https://www.coriell.org/0/Sections/Search/Sample_Detail.aspx?Ref=NA12878&product=DNA) sequenced by MGISEQ-2000 instrument

### CSV annotations 
`csv/` directory contains [CSV file](csv/reads_giab.csv) with sample metadata that was built manually according to SRA csv schema from the [example](../docs/sra_reads_annotations_example.csv).
This CSV file describes dataset of **4 paired runs** with total FASTQ size - **850.7 GB**.

### Preparing environment
Following steps should be performed to prepare your GCP environment: 
1. Make sure you have created [Google Cloud Project](https://console.cloud.google.com) and linked it to a billing account.
Store project id into your shell session with the following command: 
    ```
    PROJECT_ID=`gcloud config get-value project`
    ```
2. Create a GCS bucket for source data:
     ```
    SRC_BUCKET_NAME=${PROJECT_ID}-src
    gsutil mb gs://${SRC_BUCKET_NAME}/
    ```
3. Create a GCS bucket for intermediate results:
    ```
    WORKING_BUCKET_NAME=${PROJECT_ID}-working
    gsutil mb gs://${WORKING_BUCKET_NAME}/
    ```
4. Set GCS uri for intermediate results:
    ```
    OUTPUT_GCS_URI=gs://${WORKING_BUCKET_NAME}/processing_output/
    ```
5. Create a [BigQuery](https://cloud.google.com/bigquery) dataset with name `BQ_DATASET`:
```bash
BQ_DATASET=popgen # customize dataset name
bq mk ${PROJECT_ID}:${BQ_DATASET}
```
### Retrieving data from GCS bucket
The simpliest way is to use already prepared data stored in a [Requester Pays GCS Bucket](https://cloud.google.com/storage/docs/using-requester-pays):
    ```bash
    gs://dataflow-genomics-giab-demo/
    ```
- `gs://dataflow-genomics-giab-demo/fastq/` contains the source FASTQ runs files stored with `fastq/<SRA_Study>/<SRA_Sample>/<RUN_FASTQ>` pattern where:
     - `<SRA_Study>` - SRA study name from [CSV file](csv/reads_giab.csv)
     - `<SRA_Sample>` - SRA sample name from [CSV file](csv/reads_giab.csv)
     - `<RUN_FASTQ>` - run FASTQ file
-  `gs://dataflow-genomics-giab-demo/reference/` contains GRCh38 latest genome reference and its index
-  `gs://dataflow-genomics-giab-demo/sra-csv/` contains copy of the [CSV file](csv/reads_giab.csv)

### (Optional) Retrieving data from the original sources
Alternatively, FASTQ runs could be downloaded from NCPI ftp `ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/NA12878/MGISEQ/`.
Reference FASTA file could be retrieved from [Human Genome Resources at NCBI](https://www.ncbi.nlm.nih.gov/genome/guide/human/). 
Here is [link](ftp://ftp.ncbi.nlm.nih.gov/refseq/H_sapiens/annotation/GRCh38_latest/refseq_identifiers/GRCh38_latest_genomic.fna.gz) to GRCh38 latest genome reference. 
Reference size - 3.09 GB. Also, you must create reference index `.fai` file. 
For indexing could be used [SAM Tools utilities](http://samtools.sourceforge.net/).
If you are going to use [GATK Haplotaype Caller](https://gatk.broadinstitute.org/hc/en-us/articles/360037225632-HaplotypeCaller) there also should be reference sequence dictionary file (`.dict`).
[Here](https://gatk.broadinstitute.org/hc/en-us/articles/360037068312-CreateSequenceDictionary-Picard-) you can find an example how to create it with a GATK command-line util. 

### Data placement
1. Data should be placed in your `SRC_BUCKET_NAME` GCS bucket. You could simply copy it from `gs://dataflow-genomics-giab-demo/` bucket with following commands:
    ```
    gsutil -u ${PROJECT_ID} cp -r gs://dataflow-genomics-giab-demo/fastq gs://${SRC_BUCKET_NAME}/
    gsutil -u ${PROJECT_ID} cp -r gs://dataflow-genomics-giab-demo/reference gs://${SRC_BUCKET_NAME}/
    gsutil -u ${PROJECT_ID} cp -r gs://dataflow-genomics-giab-demo/sra_csv gs://${SRC_BUCKET_NAME}/
    ```
    If you [downloaded data from original sources](#optional-retrieving-data-from-original-sources) you should store the data according following scheme:
    ```lang-none
    + ${SRC_BUCKET_NAME}/
        + sra_csv/
            - reads_giab.csv
        + fastq/
            + GIAB-MGISEQ/
                + NA12878/
                    - V100002698_L01_1.fastq
                    - V100002698_L01_2.fastq
                    - V100002698_L02_1.fastq
                    - V100002698_L02_2.fastq
                    - V100003043_L01_1.fastq
                    - V100003043_L01_2.fastq
                    - V100003043_L02_1.fastq
                    - V100003043_L02_2.fastq
        + reference/
            - GRCh38_latest_genomic.fasta
            - GRCh38_latest_genomic.fasta.fai
            - GRCh38_latest_genomic.dict
    ```
2. Store into variables:
    ```bash
    CSV_URI=gs://${SRC_BUCKET_NAME}/sra_csv/reads_giab.csv
    REFERENCE_URI=gs://${SRC_BUCKET_NAME}/reference/GRCh38_latest_genomic.fasta
    REFERENCE_INDEX_URI=gs://${SRC_BUCKET_NAME}/reference/GRCh38_latest_genomic.fasta.fai
    ```
4. JSON string with reference data should be created for running pipeline : 
    ```
    REFERENCE_SHORT_NAME=$(echo $REFERENCE_URI| cut -f 5 -d '/' | cut -f 1 -d '.')
    REFERENCE_DATA_JSON_STRING='[{"name":"'$REFERENCE_SHORT_NAME'","fastaUri":"'$REFERENCE_URI'","indexUri":"'$REFERENCE_INDEX_URI'"}]'
    ```

### GCP settings
1. Here are suggested GCP settings (check your GCP quota for availability):
    ```
    REGION=us-central1
    MACHINE_TYPE=n1-highmem-4    
    MAX_WORKERS=100
    DISK_SIZE=100
    ```

2. Set temp and staging location for Dataflow files:
    ```
    TEMP_LOC=gs://${WORKING_BUCKET_NAME}/dataflow/temp/
    STAGING_LOC=gs://${WORKING_BUCKET_NAME}/dataflow/staging/
    ```
3. Use [Dataflow Shuffle](https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#cloud-dataflow-shuffle) for faster execution time, better autoscaling and reduction of used resources
    ```
    EXPERIMENTS=shuffle_mode=service
    ```

### BigQuery settings
To add export of VCF files into BigQuery following parameter should be added:
    ```bash
    VCF_BQ_TABLE_PATH_PATTERN=${BQ_DATASET}.GENOMICS_VARIATIONS_%s
    ```

### (Optional) Deep Variant settings
By default, pipeline uses [GATK Haplotaype Caller](https://gatk.broadinstitute.org/hc/en-us/articles/360037225632-HaplotypeCaller).
If you want to run the pipeline with a [Deep Variant](https://github.com/google/deepvariant) variant caller you should add following settings parameters:
```
VARIANT_CALLER=DEEP_VARIANT
CONTROL_PIPELIE_WORKER_REGION=$REGION
STEPS_WORKER_REGION=$REGION
M_E_CORES=16
C_V_CORES=16
P_V_CORES=16   
M_E_MEMORY=80
C_V_MEMORY=80
P_V_MEMORY=80
M_E_DISK=$DISK_SIZE
C_V_DISK=$DISK_SIZE
P_V_DISK=$DISK_SIZE
M_E_WORKERS=16
C_V_WORKERS=16
M_E_SHARDS=$M_E_WORKERS
```

### Running pipeline
To run pipeline in `FASTQ => Vcf in BigQuery` mode you should call from current directory:
```bash
mvn clean package
java -cp target/giab-example-1.0.0.jar \
    com.google.allenday.genomics.core.example.GiabExampleApp \
        --project=$PROJECT_ID \
        --runner=DataflowRunner \
        --region=$REGION \
        --inputCsvUri=$CSV_URI \
        --workerMachineType=$MACHINE_TYPE \
        --maxNumWorkers=$MAX_WORKERS \
        --numWorkers=$MAX_WORKERS \
        --diskSizeGb=$DISK_SIZE \
        --experiments=$EXPERIMENTS \
        --stagingLocation=$STAGING_LOC \
        --tempLocation=$TEMP_LOC \
        --srcBucket=$SRC_BUCKET_NAME \
        --refDataJsonString=$REFERENCE_DATA_JSON_STRING \
        --outputGcsUri=$OUTPUT_GCS_URI \
        --vcfBqDatasetAndTablePattern=$VCF_BQ_TABLE_PATH_PATTERN \
        --withFinalMerge=false
```
Last parameter `--withFinalMerge=false` means there no need to merge chunks BAM files into final sample BAM.

### (Optional) Running pipeline with DeepVariant 
Here is an example of the command for running pipeline with DeepVariant caller:

```bash
mvn clean package
java -cp target/giab-example-1.0.0.jar \
    com.google.allenday.genomics.core.example.GiabExampleApp \
        --project=$PROJECT_ID \
        --runner=DataflowRunner \
        --region=$REGION \
        --inputCsvUri=$CSV_URI \
        --workerMachineType=$MACHINE_TYPE \
        --maxNumWorkers=$MAX_WORKERS \
        --numWorkers=$MAX_WORKERS \
        --diskSizeGb=$DISK_SIZE \
        --experiments=$EXPERIMENTS \
        --stagingLocation=$STAGING_LOC \
        --tempLocation=$TEMP_LOC \
        --srcBucket=$SRC_BUCKET_NAME \
        --refDataJsonString=$REFERENCE_DATA_JSON_STRING \
        --variantCaller=$VARIANT_CALLER \
        --outputGcsUri=$OUTPUT_GCS_URI \
        --controlPipelineWorkerRegion=$CONTROL_PIPELIE_WORKER_REGION \
        --stepsWorkerRegion=$STEPS_WORKER_REGION \
        --makeExamplesCoresPerWorker=$M_E_CORES \
        --makeExamplesRamPerWorker=$M_E_MEMORY \
        --makeExamplesDiskPerWorker=$M_E_DISK \
        --callVariantsCoresPerWorker=$C_V_CORES \
        --callVariantsRamPerWorker=$C_V_MEMORY \
        --callVariantsDiskPerWorker=$C_V_DISK \
        --postprocessVariantsCores=$P_V_CORES \
        --postprocessVariantsRam=$P_V_MEMORY \
        --postprocessVariantsDisk=$P_V_DISK \
        --makeExamplesWorkers=$M_E_WORKERS \
        --callVariantsWorkers=$C_V_WORKERS \
        --deepVariantShards=$M_E_SHARDS \
        --vcfBqDatasetAndTablePattern=$VCF_BQ_TABLE_PATH_PATTERN \
        --withFinalMerge=false
```

### Results
All intermediate results (.sam, .bam aligned results) will be stored in `WORKING_BUCKET_NAME` bucket. Here is files structure of results:
```
+ ${WORKING_BUCKET_NAME}/
    + processing_output/
        + <processing_date>/
            + final/
                - (final_resluts_files)
            + intermediate/
                - (intermediate_resluts_files)
```
VCF results files will be stored in `gs://${WORKING_BUCKET_NAME}/processing_output/<processing_date>/final/result_variant_calling/` directory.

BigQuery VCF data will be stored in `${PROJECT_ID}:${BQ_DATASET}.GENOMICS_VARIATIONS_${REFERENCE_NAME}` table.


