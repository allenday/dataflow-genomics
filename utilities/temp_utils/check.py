import logging
import datetime
import csv

from google.cloud import storage


def exists_blob(bucket_name, blob_name, project):
    bucket = gcs_client.bucket(bucket_name, project)
    blob = bucket.blob(blob_name)
    return blob.exists()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    gcs_client = storage.Client("rice-3k")


    with open("rice-3k-fastq-list.txt", "r") as f:
        file_list = f.readlines()
    file_list = [name.replace("\n", "") for name in file_list]

    with open("PRJEB6180_annotations.csv", "r") as f:
        reader = csv.reader(f, delimiter='\t')

        line_count = 0
        not_exists = []
        for line in reader:
            if line_count == 0:
                run_index = line.index("Run")
                sra_study_index = line.index("SRA_Study")
                sra_sample_index = line.index("SRA_Sample")
                library_layout_index = line.index("LibraryLayout")
            else:
                logging.info("{} Processing {}".format(datetime.datetime.now(), line_count))

                path = "gs://rice-3k/PRJEB6180/fastq/{}/{}/{}".format(line[sra_study_index], line[sra_sample_index],
                                                                      line[run_index])

                for index in range(1, 3):
                    file_path = path + "_{}.fastq".format(index)
                    if file_path in file_list:
                        file_list.remove(file_path)
                    else:
                        logging.info("Not exists: {}".format(file_path))
                        not_exists.append(file_path)
            line_count += 1
    file_list = [name+"\n" for name in file_list]
    with open("diff_files.txt", "w") as f:
        f.writelines(file_list)

    not_exists = [name+"\n" for name in not_exists]
    with open("not_exis_files.txt", "w") as f:
        f.writelines(not_exists)
