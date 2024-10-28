"""
This module provides classes and functions for handling download jobs,
including validation, processing, and saving of downloaded data.
"""

import base64
import enum
import hashlib
import os
from abc import ABC, abstractmethod
from datetime import datetime as dt
from typing import List
from urllib.parse import urlsplit

import urllib3

import wis2downloader.downloader.storage as storage
from wis2downloader import stop_event
from wis2downloader.log import LOGGER
from wis2downloader.metrics import (FAILED_DOWNLOADS_BROKER, DOWNLOADED_BYTES, DOWNLOADED_FILES, FAILED_DOWNLOADS)
from wis2downloader.wis_queue import BaseQueue

MEDIA_TYPE_MAP = {
    "application/x-bufr": "bufr",
    "application/octet-stream": "bin",
    "application/xml": "xml",
    "image/jpeg": "jpeg",
    "application/x-grib": "grib",
    "application/grib;edition=2": "grib",
    "text/plain": "txt"
}


class BaseDownloader(ABC):
    """
    Abstract base class for download workers.
    """

    @abstractmethod
    def start(self):
        """
        Start the download worker to process messages from the wis_queue indefinitely.
        """

    @abstractmethod
    def process_job(self, job):
        """
        Process a single job from the wis_queue.
        """

    @abstractmethod
    def get_topic_and_centre(self, job):
        """
        Extract the topic and centre id from the job.
        """

    @abstractmethod
    def get_hash_info(self, job):
        """
        Extract the hash value and function from the job to be used for verification later.
        """

    @abstractmethod
    def get_download_url(self, job):
        """
        Extract the download url, update status, and file type from the job links.
        """

    @abstractmethod
    def extract_filename(self, _url):
        """
        Extract the filename and extension from the download link.
        """

    @abstractmethod
    def validate_data(self, data, expected_hash, hash_function, expected_size, **kwargs):
        """
        Validate the hash and size of the downloaded data against the expected values.
        """

    @abstractmethod
    def save_file(self, data, target, filename, filesize):
        """
        Save the downloaded data to disk.
        """


class VerificationMethods(enum.Enum):
    """
    Enumeration of supported hash verification methods.
    """
    SHA256 = 'sha256'
    SHA384 = 'sha384'
    SHA512 = 'sha512'
    SHA3_256 = 'sha3_256'
    SHA3_384 = 'sha3_384'
    SHA3_512 = 'sha3_512'


class DownloadWorker(BaseDownloader):
    """
    Concrete implementation of BaseDownloader for handling download jobs.
    """
    __DOWNLOAD_OPTIONS = ['fs', 's3']

    def __init__(self,
                 queue: BaseQueue,
                 download_options: dict):
        """
        Initialize the DownloadWorker.

        :param queue: The queue from which to fetch download jobs.
        :param download_options: Configuration options for downloading.
        """
        timeout = urllib3.Timeout(connect=1.0)
        self.http = urllib3.PoolManager(timeout=timeout)
        self.queue = queue
        self.status = "ready"
        self.download_options = self.__validate_download_options(download_options)

    def __validate_download_options(self, options: dict):
        """
        Validate the download options.

        :param options: The download options to validate.
        :raises ValueError: If any required option is missing or invalid.
        """
        d_ops: List[storage.Storage] = []
        for key in self.__DOWNLOAD_OPTIONS:
            if key not in options:
                raise ValueError(f"Missing required download option: {key}")
            d_ops.append(getattr(storage, key.upper())(**options[key]))

        return d_ops

    def start(self) -> None:
        """
        Start the download worker to process messages from the wis_queue indefinitely.
        """
        LOGGER.info("Starting download worker")
        while not stop_event.is_set():
            job = self.queue.dequeue()
            if job.get('shutdown', False):
                break

            self.status = "running"
            try:
                self.process_job(job)
            except Exception as e:
                LOGGER.error(e)

            self.status = "ready"
            self.queue.task_done()

    def process_job(self, job) -> None:
        """
        Process a single job from the wis_queue.

        :param job: The job to process.
        """

        expected_hash, hash_function = self.get_hash_info(job)
        expected_size = job.get('payload', {}).get('content', {}).get('size')

        # Get the download url, update status, and file type from the job links
        _url, update, media_type = self.get_download_url(job)

        if _url is None:
            LOGGER.warning("No download link found in job %s", job)
            return

        # Map media type to file extension
        file_type = MEDIA_TYPE_MAP.get(media_type, 'bin')

        # Global caches can set whatever filename they want, we need to use
        # the data_id for uniqueness. However, this can be unwieldy, hence use
        # the hash of data_id
        # TODO use data_id to store on Minio S3
        data_id = job.get('payload', {}).get('properties', {}).get('data_id')
        filename, _ = self.extract_filename(_url)
        filename = f"{filename}.{file_type}"
        # loop through storage adapters to assign full download location
        for storage_obj in self.download_options:
            if hasattr(storage_obj, "base_path"):
                storage_obj.base_path = job.get("target", ".")
            elif hasattr(storage_obj, "s3_path"):
                storage_obj.s3_path = data_id

        # Get information needed for download metric labels
        topic, centre_id = self.get_topic_and_centre(job)

        # Start timer of download time to be logged later
        download_start = dt.now()

        # Download the file
        try:
            response = self.http.request('GET', _url)
            # Get the file size in KB
            filesize = len(response.data)
            # Use the hash function to determine whether to save the data
            if not response or not self.validate_data(response.data,
                                                      expected_hash,
                                                      hash_function,
                                                      expected_size,
                                                      data_id=data_id):
                FAILED_DOWNLOADS_BROKER.labels(topic=topic, centre_id=centre_id).inc(1)
                return
        except Exception as e:
            LOGGER.error("Error downloading %s", _url)
            LOGGER.error(e)
            # Increment failed download counter
            FAILED_DOWNLOADS_BROKER.labels(topic=topic, centre_id=centre_id).inc(1)
            return

        file_type_label = file_type if file_type in ['bufr', 'grib', 'json', 'xml', 'png'] else 'other'
        # Save using Multiple storage adapters
        for storage_obj in self.download_options:
            result = storage_obj.save(
                response.data,
                filename,
                filesize,
                "application/octet-stream")
            if result:
                DOWNLOADED_BYTES.labels(
                    target=storage_obj.__name__,
                    topic=topic,
                    centre_id=centre_id,
                    file_type=file_type_label).inc(filesize)
                DOWNLOADED_FILES.labels(
                    target=storage_obj.__name__,
                    topic=topic,
                    centre_id=centre_id,
                    file_type=file_type_label).inc(1)
            else:
                FAILED_DOWNLOADS.labels(
                    target=storage_obj.__name__,
                    topic=topic,
                    centre_id=centre_id).inc(1)

        download_time = (dt.now() - download_start).total_seconds()
        LOGGER.info("Downloaded %s of size %d bytes in %.2f seconds", filename, filesize, download_time)

    def get_topic_and_centre(self, job) -> tuple:
        """
        Extract the topic and centre id from the job.

        :param job: The job to extract information from.
        :returns: A tuple containing the topic and centre id.
        """
        topic = job.get('topic')
        return topic, topic.split('/')[3]

    def get_hash_info(self, job):
        """
        Extract the hash value and function from the job for verification.

        :param job: The job to extract information from.
        :returns: A tuple containing the expected hash and hash function.
        """
        expected_hash = job.get('payload', {}).get('properties', {}).get('integrity', {}).get('hash')
        hash_method = job.get('payload', {}).get('properties', {}).get('integrity', {}).get('method')
        # Check if hash method is known using our enumeration of hash methods
        try:
            method = VerificationMethods[hash_method].value
            hash_function = hashlib.new(method)
        except KeyError:
            LOGGER.warning("Unknown hash method: %s", hash_method)
            hash_function = None

        return expected_hash, hash_function

    def get_download_url(self, job) -> tuple:
        """
        Extract the download url, update status, and file type from the job links.

        :param job: The job to extract information from.
        :returns: A tuple containing the download url, update status, and media type.
        """
        links = job.get('payload', {}).get('links', [])
        _url, update, media_type = None, False, None
        for link in links:
            if link.get('rel') == 'update':
                _url, media_type, update = link.get('href'), link.get('type'), True
                break
            elif link.get('rel') == 'canonical':
                _url, media_type = link.get('href'), link.get('type')
                break

        return _url, update, media_type

    def extract_filename(self, _url) -> tuple:
        """
        Extract the filename and extension from the download link.

        :param _url: The download url.
        :returns: A tuple containing the filename and extension.
        """
        path = urlsplit(_url).path
        filename = os.path.basename(path)
        return os.path.splitext(filename)

    def validate_data(
            self,
            data,
            expected_hash,
            hash_function,
            expected_size,
            **kwargs) -> bool:
        """
        Validate the hash and size of the downloaded data against the expected values.

        :param data: The downloaded data.
        :param expected_hash: The expected hash value.
        :param hash_function: The hash function to use for validation.
        :param expected_size: The expected size of the data.
        :returns: True if the data is valid, False otherwise.
        """
        if None in (expected_hash, hash_function):
            return True
        hash_value = base64.b64encode(hash_function(data).digest()).decode()
        if not (hash_value == expected_hash and len(data) == expected_size):
            LOGGER.warning("Download %s failed verification, discarding", kwargs.get("data_id"))
            return False
        return True

    def save_file(self, data, target, filename, filesize) -> None:
        """
        Save the downloaded data to disk.

        :param data: The downloaded data.
        :param target: The target path to save the data.
        :param filename: The name of the file.
        :param filesize: The size of the file.
        :param download_start: The start time of the download.
        """
        try:
            target.write_bytes(data)
        except Exception as e:
            LOGGER.error("Error saving to disk: %s", target)
            LOGGER.error(e)
