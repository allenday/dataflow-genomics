import os
import sys
from os import listdir

import logging
import requests
import datetime

from google.cloud import storage


def exists_blob(bucket_name, blob_name, project):
    bucket = client.bucket(bucket_name, project)
    blob = bucket.blob(blob_name)
    return blob.exists()


def download_file_from_gcs(uri, project):
    main = uri.split("//")[-1]
    bucket_name = main.split("/")[0]
    blob_name = main.replace(bucket_name + "/", "")
    bucket = client.bucket(bucket_name, project)
    blob = bucket.blob(blob_name)
    logging.info("Downloading from GCS {}...".format(uri))
    dest_path = blob_name.split("/")[-1].replace(".1", "")
    blob.download_to_filename(blob_name.split("/")[-1].replace(".1", ""))
    logging.info("Downloaded {} into {}".format(uri, dest_path))
    return dest_path


def download_file_from_http(uri):
    local_filename = uri.split('/')[-1].replace(".1", "")
    # NOTE the stream=True parameter below
    logging.info("Downloading from HTTP {}...".format(uri))

    with requests.get(uri, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    # f.flush()
    logging.info("Downloaded {} into {}".format(uri, local_filename))
    return local_filename


def run_command(command):
    import subprocess
    logging.info("Running sh command: {}".format(command))
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    return output, error


def upload_file(filename, destination_bucket_name, destination_blob_name):
    """
    Uploads a file to a given Cloud Storage bucket and returns the public url
    to the new object.
    """
    bucket = storage.Client().get_bucket(destination_bucket_name)
    blob = bucket.blob(destination_blob_name)
    logging.info("Uploading {} to gs://{}/{}...".format(filename, dest_bucket, destination_blob_name))
    blob.upload_from_filename(
        filename,
        content_type="text/plain")
    logging.info("Uploading {} to gs://{}/{} finished".format(filename, dest_bucket, destination_blob_name))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    if len(sys.argv) < 5:
        logging.info(
            'Script must be called with 4 arguments - project, source_filename, dest_bucket, path_to_sra_toolkit_dir')
        logging.info(
            'Example of the source file\'s content: SRP072226,SRS1357154,SRR3286322,gs://sra-pub-run-1/SRR3286322/SRR3286322.1')
        exit(1)

    project = sys.argv[1]
    source_filename = sys.argv[2]
    dest_bucket = sys.argv[3]
    path_to_sra_toolkit_dir = sys.argv[4]

    client = storage.Client(project)

    work_dir = "temp"

    counter = 0
    with open(source_filename, "r") as f:
        while True:
            line = f.readline().replace("\n", "")
            if line:
                counter += 1
                logging.info("{} Processing {}".format(datetime.datetime.now(), counter))
                csw_row = line.split(",")
                uri = csw_row[3]
                if uri.startswith("gs"):
                    local_path = download_file_from_gcs(uri, project)
                elif uri.startswith("http"):
                    local_path = download_file_from_http(uri)
                else:
                    continue

                output, error = run_command(
                    "./{}/bin/fastq-dump {} --split-files -O {}".format(path_to_sra_toolkit_dir, local_path,
                                                                        work_dir))

                paths = [path for path in listdir(work_dir + "/")]
                for path in paths:
                    dest_blob_name = "sra/{}/{}/{}".format(csw_row[0], csw_row[1], path)
                    upload_file("{}/{}".format(work_dir, path), dest_bucket, dest_blob_name)
                os.remove(local_path)
                import shutil

                shutil.rmtree(work_dir)
            else:
                break
