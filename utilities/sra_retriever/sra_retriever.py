import sys
import logging
import json
import datetime
import os
import subprocess

from os import listdir
from subprocess import TimeoutExpired

import shutil
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import logging as glogging


def log_info(text):
    logging.info(text)
    logger.log_text(text)


def log_error(error):
    logging.exception(error)
    logging.info(str(error))
    logger.log_text(str(error))


def ack_msg(subscription_path, ack_id):
    log_info("Ack: {}".format(ack_id))
    subscriber.acknowledge(subscription_path, [ack_id])


def publish_msg(topic_path, data):
    log_info("Publish: {}".format(data))
    # When you publish a message, the client returns a future.
    response = pulisher_client.publish(
        topic_path, data=data.encode("utf-8")  # data must be a bytestring.
    )
    logging.info(response)


def process_pubsub_msg(subscription_path, ack_id, message, success_topic, failed_topic):
    log_info("Received message: {}".format(message))
    ack_msg(subscription_path, ack_id)

    data = message.data
    log_info("Message data: {}".format(data))
    start = datetime.datetime.now()
    try:
        process_sra_msg(data)
        finish = datetime.datetime.now()
        log_info("finish, {}".format(success_topic))
        if success_topic:
            delta = (finish - start).total_seconds()
            publish_msg(success_topic, "Finished in: {}, {}".format(str(delta), data))
    except Exception as e:
        log_error(e)
        if failed_topic:
            publish_msg(failed_topic, data.decode("utf-8"))


def process_sra_msg(data):
    json_data = json.loads(data)

    log_info("{} Processing {}".format(datetime.datetime.now(), data))

    sra_study = json_data["sra_study"]
    sra_sample = json_data["sra_sample"]
    run = json_data["run"]

    dest_path_pattern = "{}{}/{}/{}"

    if exists_blob(dest_bucket, dest_path_pattern.format(dest_dir, sra_study, sra_sample, "{}_1.fastq".format(run)),
                   project):
        return

    output, error = run_command(
        "fastq-dump {} --split-files --skip-technical -O {}".format(run, work_dir))

    if error:
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir)
        raise Exception(error)
    else:
        paths = [path for path in listdir(work_dir + "/")]
        for path in paths:
            dest_blob_name = dest_path_pattern.format(dest_dir, sra_study, sra_sample, path)
            upload_file("{}/{}".format(work_dir, path), dest_bucket, dest_blob_name)
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir)


def run_command(command):
    log_info("Running sh command: {}".format(command))

    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    try:
        output, error = process.communicate(timeout=36000)
    except TimeoutExpired as e:
        output = None
        error = str(e)
    log_info("Finished {}. Output:{}. Error: {}".format(command, output, error))
    return output, error


def upload_file(filename, destination_bucket_name, destination_blob_name):
    """
    Uploads a file to a given Cloud Storage bucket and returns the public url
    to the new object.
    """
    bucket = storage.Client().bucket(destination_bucket_name)
    blob = bucket.blob(destination_blob_name)
    log_info("Uploading of {} to gs://{}/{}...".format(filename, dest_bucket, destination_blob_name))
    blob.upload_from_filename(
        filename,
        content_type="text/plain")
    log_info("Finished uploading of {} to gs://{}/{}".format(filename, dest_bucket, destination_blob_name))


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
        log_info(
            'Script must be called with 3 arguments - project, subscription, dest_gcs_dir_uri')
        exit(1)

    project = sys.argv[1]
    subscription = sys.argv[2]
    dest_gcs_dir_uri = sys.argv[3]

    pulisher_client = pubsub_v1.PublisherClient()

    success_topic = pulisher_client.topic_path(project, sys.argv[4])
    failed_topic = pulisher_client.topic_path(project, sys.argv[5])

    subscriber = pubsub_v1.SubscriberClient()
    logging_client = glogging.Client()
    logger = logging_client.logger("sra_retriever_logs")

    gcs_client = storage.Client(project=project)

    dest_bucket, dest_dir = parse_file_to_bucket_and_filename(dest_gcs_dir_uri)
    log_info("dest_bucket:{}, dest_dir:{}".format(dest_bucket, dest_dir))
    work_dir = "temp"

    subscription_path = subscriber.subscription_path(project, subscription)

    while True:
        response = subscriber.pull(subscription_path, max_messages=1, timeout=1200)

        log_info("Received {} messages".format(len(response.received_messages)))
        counter = 0
        for received_message in response.received_messages:
            message = received_message.message
            process_pubsub_msg(subscription_path, received_message.ack_id, message, success_topic, failed_topic)
            counter += 1
        if counter == 0:
            break
