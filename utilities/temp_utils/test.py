import sys
import logging
import json
import datetime
import os

from os import listdir

import shutil
import time

from google.cloud import storage
from google.cloud import pubsub_v1

def ack_msg(message):
    logging.info("{} Ack: {}".format(datetime.datetime.now(), str(message)))
    message.ack()

def callback(message):
    logging.info("Received message: {}".format(message))
    data = message.data
    logging.info("Message data: {}".format(data))
    try:
        time.sleep(15)
        ack_msg(message)
    except Exception as e:
        logging.error(str(e))



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
    subscriber = pubsub_v1.SubscriberClient()
    gcs_client = storage.Client("rice-3k")

    subscription_path = subscriber.subscription_path("rice-3k", "sra-retriever-subscription")
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)

    # streaming_pull_future = subscriber.subscribe(
    #     subscription_path, callback=callback, flow_control=flow_control
    # )
    while True:
        response = subscriber.pull(
            subscription_path, max_messages=1
        )
        logging.info(response)

        for received_message in response.received_messages:
            message = received_message.message
            logging.info("Received message: {}".format(message))
            data = message.data
            logging.info("Message data: {}".format(data))
            try:
                time.sleep(15)
                ack_msg(message)
            except Exception as e:
                logging.error(str(e))
    # logging.info("Listening for messages on {}..\n".format(subscription_path))
    #
    # # result() in a future will block indefinitely if `timeout` is not set,
    # # unless an exception is encountered first.
    # try:
    #     streaming_pull_future.result()
    # except Exception as e:
    #     logging.error(str(e))
    #     streaming_pull_future.cancel()
    # while True:
    #     time.sleep(5)
