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
    def extract_file_name(self, _url):
        """
        Extract the file_name and extension from the download link.
        """

    @abstractmethod
    def validate_data(self, data, expected_hash, hash_function, expected_size, **kwargs):
        """
        Validate the hash and size of the downloaded data against the expected values.
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
        self.download_options = [getattr(storage, key.upper())(**download_options[key]) for key in download_options]

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
                raise e

            self.status = "ready"
            self.queue.task_done()

    def process_job(self, job) -> None:
        """
        Process a single job from the wis_queue.

        :param job: The job to process.
        """

        expected_hash, hash_function = self.get_hash_info(job)
        # FIXME BUG payload has no content
        payload = job.get('payload', {})
        expected_size = (payload.get('content', {}).get('size') or
                         payload.get('properties', {}).get('content', {}).get('size'))

        # Get the download url, update status, and file type from the job links
        _url, update, media_type = self.get_download_url(job)

        if _url is None:
            LOGGER.warning("No download link found in job %s", job)
            return

        # Start timer of download time to be logged later
        # TODO complete download timeit
        download_start = dt.now()

        # Get information needed for download metric labels
        topic, centre_id = self.get_topic_and_centre(job)
        # Download the file
        try:
            response = self.http.request('GET', _url)
            # Get the file size in KB
            file_size = len(response.data)
            # Global caches can set whatever file_name they want, we need to use
            # the data_id for uniqueness. However, this can be unwieldy, hence use
            #  the hash function to determine whether to save the data
            data_id = job.get('payload', {}).get('properties', {}).get('data_id')

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

        # Map media type to file extension
        file_type = MEDIA_TYPE_MAP.get(media_type, 'bin')
        file_name, _ = self.extract_file_name(_url)
        # Save using Multiple storage adapters
        for storage_obj in self.download_options:
            result = storage_obj.save(
                response.data,
                storage.StorageKeyType[storage_obj.__name__].build_key(job=job, file_name=f"{file_name}.{file_type}"),
                file_size,
                "application/octet-stream")
            if result:
                # Get File label
                file_type_label = file_type if file_type in ['bufr', 'grib', 'json', 'xml', 'png'] else 'other'
                DOWNLOADED_BYTES.labels(target=storage_obj.__name__, topic=topic, centre_id=centre_id,
                                        file_type=file_type_label).inc(file_size)
                DOWNLOADED_FILES.labels(target=storage_obj.__name__, topic=topic, centre_id=centre_id,
                                        file_type=file_type_label).inc(1)
            else:
                FAILED_DOWNLOADS.labels(target=storage_obj.__name__, topic=topic, centre_id=centre_id).inc(1)

        download_time = (dt.now() - download_start).total_seconds()
        LOGGER.info("Downloaded %s of size %d bytes in %.2f seconds", file_name, file_size, download_time)

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
        # FIXME BUG integrity has no hash (value instead)
        integrity = job.get('payload', {}).get('properties', {}).get('integrity', {})
        expected_hash = integrity.get('hash') or integrity.get('value')
        hash_method = job.get('payload', {}).get('properties', {}).get('integrity', {}).get('method')
        # Check if hash method is known using our enumeration of hash methods
        try:
            method = VerificationMethods[hash_method.upper()].value
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

    def extract_file_name(self, _url) -> tuple:
        """
        Extract the file_name and extension from the download link.

        :param _url: The download url.
        :returns: A tuple containing the file_name and extension.
        """
        path = urlsplit(_url).path
        file_name = os.path.basename(path)
        # FIXME is it a BUG ?? some filename has naming like this below, do we keep it this way ?
        #  eaa5cc04-f9c1-4493-afda-fa096eafdb12__WIGOS_0-20000-0-60693_20241018T210000.bufr4
        return os.path.splitext(file_name)

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
        # TODO BUG TypeError: '_hashlib.HASH' object is not callable in
        #  hash_value = base64.b64encode(hash_function(data).digest()).decode(), hash_function is an instance and
        #  should use its update method
        hash_function.update(data)
        hash_value = base64.b64encode(hash_function.digest()).decode()
        if not (hash_value == expected_hash and len(data) == expected_size):
            LOGGER.warning("Download %s failed verification, discarding", kwargs.get("data_id"))
            return False
        return True
