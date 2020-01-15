import sys

import logging
import datetime
import csv
import json
import time

from google.cloud import pubsub
futures = dict()


def get_callback(f, data):
    def callback(f):
        try:
            logging.info(f.result())
            futures.pop(data)
        except:  # noqa
            logging.error("Please handle {} for {}.".format(f.exception(), data))
    return callback


def publish_msg(topic_path, data):
    futures.update({data: None})
    # When you publish a message, the client returns a future.
    future = pulisher_client.publish(
        topic_path, data=data.encode("utf-8")  # data must be a bytestring.
    )
    futures[data] = future
    # Publish failures shall be handled in the callback function.
    future.add_done_callback(get_callback(future, data))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    if len(sys.argv) < 4:
        logging.info(
            'Script must be called with 3 arguments - project, source_filename, dest_topic')
        exit(1)

    project = sys.argv[1]
    source_filename = sys.argv[2]
    dest_topic = sys.argv[3]

    pulisher_client = pubsub.PublisherClient()
    topic_path = pulisher_client.topic_path(project, dest_topic)

    counter = 0
    with open(source_filename, "r") as f:
        reader = csv.reader(f, delimiter='\t')
        line_count = 0

        for line in reader:
            if line_count == 0:
                run_index = line.index("Run")
                sra_study_index = line.index("SRA_Study")
                sra_sample_index = line.index("SRA_Sample")
                library_layout_index = line.index("LibraryLayout")
            else:
                logging.info("{} Processing {}".format(datetime.datetime.now(), line_count))

                publish_msg(topic_path, json.dumps(
                    {"run": line[run_index], "sra_study": line[sra_study_index], "sra_sample": line[sra_sample_index]}))
                break
            line_count += 1
    # Wait for all the publish futures to resolve before exiting.
    while futures:
        logging.info("Futures len: {}".format(len(futures)))
        time.sleep(5)
    logging.info("Futures len: {}".format(len(futures)))

