import logging
import datetime
import csv

from google.cloud import storage

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    gcs_client = storage.Client("rice-3k")

    with open("rice-3k-fastq-list.txt", "r") as f:
        file_list = f.readlines()
    file_list = [name.replace("\n", "") for name in file_list]
    file_list = [tuple(name.split("  ")) for name in file_list]
    file_list = [(int(name_tuple[0]), name_tuple[1], name_tuple[2]) for name_tuple in file_list if name_tuple[0]]
    sorted_file_list = sorted(file_list, key=lambda tup: tup[0], reverse=True)

    for i in range(25000):
        if i % 1000 == 0:
            logging.info(str(i + 1) + " " + str(sorted_file_list[i]))
