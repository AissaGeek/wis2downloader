import argparse
import json
import logging
import os
import threading

from flask import Flask, request

from wis2downloader import shutdown
from wis2downloader.log import LOGGER, setup_logger
from wis2downloader.subscriber import MQTTSubscriber, BaseSubscriber
from wis2downloader.queue import SimpleQueue, QMonitor
from wis2downloader.downloader import DownloadWorker

setup_logger("INFO")


def create_app(subscriber: BaseSubscriber):
    """
    Starts the Flask app server and enables
    the addition or deletion of topics to the
    concurrent susbcription.
    It also spawns multiple download workers to
    handle the downloading and verification of the data.

    Args:
        subscriber (BaseSubscriber): A subscriber to listen for new data
            notifications. Any subscriber derived from the base subscriber
            class.
    """
    LOGGER.debug("Creating Flask app...")

    # Create the Flask app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    # Enable adding, deleting, or listing subscriptions
    @app.route('/add')
    def add_subscription():
        # Todo - validation of args
        topic = request.args.get('topic')
        target = request.args.get('target')
        if target is None:
            target = "$TOPIC"
        return subscriber.add_subscription(topic, target)

    @app.route('/delete')
    def delete_subscription():
        # Todo - validation of args
        topic = request.args.get('topic')
        return subscriber.delete_subscription(topic)

    @app.route('/list')
    def list_subscriptions():
        return subscriber.list_subscriptions()

    return app


def main():
    """
    Main function to run the Flask app server,
    which uses the WIS2 downloader sub-modules
    (subscriber, queuer, downloader) to download
    the latest data.
    """

    # Get CLI arguments passed (currently path to config.json)
    parser = argparse.ArgumentParser(
        description="WIS2 Downloader app configuration")
    parser.add_argument(
        "--config", default="config.json",
        help="Path to the Flask app configuration file"
    )
    args = parser.parse_args()

    # Now load config settings for downloader
    with open(args.config, "r") as f:
        config = json.load(f)

    # Extract MQTT options
    broker_url = config.get("broker_url", "globalbroker.meteo.fr")
    broker_port = config.get("broker_port", 443)
    username = config.get("username", "everyone")
    password = config.get("password", "everyone")
    protocol = config.get("protocol", "websockets")

    # Download options, i.e. where to write data to, number of workers
    topics = config.get("topics", {})
    download_dir = config.get("download_dir", ".")
    num_workers = config.get("download_workers", 1)


    # Finally flask options
    flask_host = config.get("flask_host", "127.0.0.1")
    flask_port = config.get("flask_port", 5000) # find_open_port()) # port needs to be explicitly set otherwise their may be issues with the firewall.

    # Now set up the different threads (plus job Q)
    # 1) queue monitor
    # 2) download workers
    # 3) subscriber

    # create the queue
    jobQ = SimpleQueue()

    # Start the queue monitor
    Q_monitor = threading.Thread(
        target=QMonitor, args=(jobQ,), daemon=True
    )
    Q_monitor.start()

    # start workers to process the jobs from the queue
    worker_threads = []
    # basepath = "downloads"
    for idx in range(num_workers):
        worker = DownloadWorker(jobQ, download_dir)
        worker_threads.append(
            threading.Thread(target=worker.start,
                             daemon=True)
        )
        worker_threads[idx].start()


    # now create the MQTT subscriber
    subscriber = MQTTSubscriber(
        broker_url, broker_port, username, password, protocol, jobQ
    )

    # Now spawn subscriber as thread
    mqtt_thread = threading.Thread(
        target=subscriber.start, daemon=True)
    mqtt_thread.start()

    # add default subscriptions
    for topic, target in topics.items():
        subscriber.add_subscription(topic, target)

    # Now all background jobs / threads should be running start the flask
    # backend for managing the subscriptions

    # Start the Flask app
    try:
        app = create_app(subscriber=subscriber)
    except Exception as e:
        LOGGER.error(f"Error creating Flask app: {e}")

    LOGGER.info(f"Flask host: {flask_host}, flask port: {flask_port}")
    app.run(host=flask_host, port=flask_port, debug=True, use_reloader=False)

    LOGGER.info("Shutting down")
    subscriber.stop()
    shutdown.set()
    # stop threads (this needs work !!!! ToDo)
    mqtt_thread.join()
    LOGGER.info("Subscriber thread stopped")
    LOGGER.info("Stopping queue monitor, this may take 60 seconds")
    Q_monitor.join()
    LOGGER.info("Queue monitor stopped")
    for worker in worker_threads:
        LOGGER.info("Shutting down worker threads")
        if jobQ.size() == 0:
            # download worker is blocked waiting for a job, send one.
            jobQ.enqueue({'shutdown': True})
        worker.join()


if __name__ == '__main__':
    main()
