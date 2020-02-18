import sys
import logging
import json
import datetime
import os

from os import listdir

import shutil
from google.cloud import storage
from google.cloud import pubsub


def ack_msg(subscription_path, ack_id):
    subscriber.acknowledge(subscription_path, [ack_id])

def process_pubsub_msg(subscription_path, ack_id, message):
    logging.info("Received message: {}".format(message))
    data = message.data
    logging.info("Message data: {}".format(data))
    try:
        process_sra_msg(data)
        ack_msg(subscription_path, ack_id)
    except Exception as e:
        logging.exception(e)


def process_sra_msg(data):
    json_data = json.loads(data)

    logging.info("{} Processing {}".format(datetime.datetime.now(), data))

    run = json_data["run"]

    if exists_blob(dest_bucket,
                   "{}{}/{}/{}_1.fastq".format(dest_dir, json_data["sra_study"], json_data["sra_sample"], run),
                   project):
        return

    output, error = run_command(
        "fastq-dump {} --split-files -O {}".format(run, work_dir))

    if error:
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir)
        raise Exception(error)
    else:
        paths = [path for path in listdir(work_dir + "/")]
        for path in paths:
            dest_blob_name = "{}{}/{}/{}".format(dest_dir, json_data["sra_study"], json_data["sra_sample"], path)
            upload_file("{}/{}".format(work_dir, path), dest_bucket, dest_blob_name)
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir)


def run_command(command):
    import subprocess
    logging.info("Running sh command: {}".format(command))
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    logging.info("Finished {}. Output:{}. Error: {}".format(command, output, error))
    return output, error


def upload_file(filename, destination_bucket_name, destination_blob_name):
    """
    Uploads a file to a given Cloud Storage bucket and returns the public url
    to the new object.
    """
    bucket = storage.Client().bucket(destination_bucket_name)
    blob = bucket.blob(destination_blob_name)
    logging.info("Uploading of {} to gs://{}/{}...".format(filename, dest_bucket, destination_blob_name))
    blob.upload_from_filename(
        filename,
        content_type="text/plain")
    logging.info("Finished uploading of {} to gs://{}/{}".format(filename, dest_bucket, destination_blob_name))


def parse_file_to_bucket_and_filename(file_path):
    """Divides file path to bucket name and file name"""
    path_parts = file_path.split("//")
    if len(path_parts) >= 2:
        main_part = path_parts[1]
        if "/" in main_part:
            divide_index = main_part.index("/")
            bucket_name = main_part[:divide_index]
            file_name = main_part[divide_index + 1 - len(main_part):]

            return bucket_name, file_name
    return "", ""


def exists_blob(bucket_name, blob_name, project):
    bucket = gcs_client.bucket(bucket_name, project)
    blob = bucket.blob(blob_name)
    return blob.exists()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    if len(sys.argv) < 4:
        logging.info(
            'Script must be called with 3 arguments - project, subscription, dest_gcs_dir_uri')
        exit(1)

    project = sys.argv[1]
    subscription = sys.argv[2]
    dest_gcs_dir_uri = sys.argv[3]

    subscriber = pubsub.SubscriberClient()
    gcs_client = storage.Client(project=project)

    dest_bucket, dest_dir = parse_file_to_bucket_and_filename(dest_gcs_dir_uri)
    logging.info("dest_bucket:{}, dest_dir:{}".format(dest_bucket, dest_dir))
    work_dir = "temp"

    subscription_path = subscriber.subscription_path(project, subscription)

    while True:
        response = subscriber.pull(subscription_path, max_messages=1, timeout=1200)
        counter = 0
        for received_message in response.received_messages:
            message = received_message.message
            process_pubsub_msg(subscription_path, received_message.ack_id, message)
            counter += 1
        if counter == 0:
            break
