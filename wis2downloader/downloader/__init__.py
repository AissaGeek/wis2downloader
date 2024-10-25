import base64
import enum
import hashlib
import os
import shutil
from abc import ABC, abstractmethod
from datetime import datetime as dt
from pathlib import Path
from urllib.parse import urlsplit

import urllib3

from wis2downloader import stop_event
from wis2downloader.log import LOGGER
from wis2downloader.metrics import (DOWNLOADED_BYTES, DOWNLOADED_FILES,
                                    FAILED_DOWNLOADS)
from wis2downloader.wis_queue import BaseQueue


class BaseDownloader(ABC):

    @abstractmethod
    def start(self):
        """Start the download worker to
        process messages from the wis_queue indefinitely"""

    @abstractmethod
    def process_job(self, job):
        """Process a single job from the wis_queue"""

    @abstractmethod
    def get_topic_and_centre(self, job):
        """Extract the topic and centre id from the job"""

    @abstractmethod
    def get_hash_info(self, job):
        """Extract the hash value and function from the job
        to be used for verification later"""

    @abstractmethod
    def get_download_url(self, job):
        """Extract the download url, update status, and
        file type from the job links"""

    @abstractmethod
    def extract_filename(self, _url):
        """Extract the filename and extension from the download link"""

    @abstractmethod
    def validate_data(self, data, expected_hash, hash_function, expected_size):
        """Validate the hash and size of the downloaded data against
        the expected values"""

    @abstractmethod
    def save_file(self, data, target, filename, filesize, download_start):
        """Save the downloaded data to disk"""


def get_todays_date():
    """
    Returns today's date in the format yyyy/mm/dd.
    """
    today = dt.now()
    yyyy = f"{today.year:04}"
    mm = f"{today.month:02}"
    dd = f"{today.day:02}"
    return yyyy, mm, dd


def map_media_type(media_type):
    _map = {
        "application/x-bufr": "bufr",
        "application/octet-stream": "bin",
        "application/xml": "xml",
        "image/jpeg": "jpeg",
        "application/x-grib": "grib",
        "application/grib;edition=2": "grib",
        "text/plain": "txt"
    }

    return _map.get(media_type, 'bin')


class VerificationMethods(enum.Enum):
    sha256 = 'sha256'
    sha384 = 'sha384'
    sha512 = 'sha512'
    sha3_256 = 'sha3_256'
    sha3_384 = 'sha3_384'
    sha3_512 = 'sha3_512'


class DownloadWorker(BaseDownloader):
    def __init__(self, queue: BaseQueue, basepath: str = ".", min_free_space=10):  # noqa
        timeout = urllib3.Timeout(connect=1.0)
        self.http = urllib3.PoolManager(timeout=timeout)
        self.queue = queue
        self.basepath = Path(basepath)
        self.min_free_space = min_free_space * 1073741824  # GBytes
        self.status = "ready"

    def start(self) -> None:
        LOGGER.info("Starting download worker")
        while not stop_event.is_set():
            # First get the job from the wis_queue
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

    def get_free_space(self):
        _, _, free = shutil.disk_usage(self.basepath)
        return free

    def process_job(self, job) -> None:
        yyyy, mm, dd = get_todays_date()
        output_dir = self.basepath / yyyy / mm / dd

        # Add target to output directory
        output_dir = output_dir / job.get("target", ".")

        # Get information about the job for verification later
        expected_hash, hash_function = self.get_hash_info(job)
        expected_size = job.get('payload', {}).get('content', {}).get('size')

        # Get the download url, update status, and file type from the job links
        _url, update, media_type = self.get_download_url(job)

        if _url is None:
            LOGGER.warning("No download link found in job %s", job)
            return

        # map media type to file extension
        file_type = map_media_type(media_type)

        # Global caches can set whatever filename they want, we need to use
        # the data_id for uniqueness. However, this can be unwieldy, hence use
        # the hash of data_id
        # TODO use data_id to store on Minio S3
        data_id = job.get('payload', {}).get('properties', {}).get('data_id')
        filename, _ = self.extract_filename(_url)
        filename = f"{filename}.{file_type}"
        # TODO remove this (already handled)
        #     ---------------
        #
        target = output_dir / filename
        # Create parent dir if it doesn't exist
        target.parent.mkdir(parents=True, exist_ok=True)
        # Only download if file doesn't exist or is an update
        if target.is_file() and not update:
            LOGGER.info("Skipping download of %s, already exists", filename)
            return
        # TODO ---------------

        # Get information needed for download metric labels
        topic, centre_id = self.get_topic_and_centre(job)

        # Standardise the file type label, defaulting to 'other'
        file_type_label = file_type if file_type in ['bufr', 'grib', 'json', 'xml', 'png'] else 'other'

        # Start timer of download time to be logged later
        download_start = dt.now()

        # Download the file
        try:
            response = self.http.request('GET', _url)
            # Get the file size in KB
            filesize = len(response.data)
        except Exception as e:
            LOGGER.error("Error downloading %s", _url)
            LOGGER.error(e)
            # Increment failed download counter
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            return

        if self.min_free_space > 0:  # only check size if limit set
            free_space = self.get_free_space()
            if free_space < self.min_free_space:
                LOGGER.warning("Too little free space, %d < %d, file %s not saved",
                               free_space - filesize, self.min_free_space, data_id)
                FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
                return

        if response is None:
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            return

        # Use the hash function to determine whether to save the data
        if not self.validate_data(response.data, expected_hash, hash_function, expected_size):
            LOGGER.warning("Download %s failed verification, discarding", data_id)  # noqa
            # Increment failed download counter
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            return

        # Now save
        self.save_file(response.data, target, filename, filesize, download_start)

        # Increment metrics
        DOWNLOADED_BYTES.labels(topic=topic, centre_id=centre_id, file_type=file_type_label).inc(filesize)
        DOWNLOADED_FILES.labels(topic=topic, centre_id=centre_id, file_type=file_type_label).inc(1)

    def get_topic_and_centre(self, job) -> tuple:
        topic = job.get('topic')
        return topic, topic.split('/')[3]

    def get_hash_info(self, job):
        expected_hash = job.get('payload', {}).get('properties', {}).get('integrity', {}).get('hash')
        hash_method = job.get('payload', {}).get('properties', {}).get('integrity', {}).get('method')

        hash_function = None

        # Check if hash method is known using our enumeration of hash methods
        if hash_method in VerificationMethods.__members__:
            method = VerificationMethods[hash_method].value
            hash_function = hashlib.new(method)

        return expected_hash, hash_function

    def get_download_url(self, job) -> tuple:
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
        path = urlsplit(_url).path
        filename = os.path.basename(path)
        return os.path.splitext(filename)

    def validate_data(self, data, expected_hash, hash_function, expected_size) -> bool:
        if None in (expected_hash, hash_function):
            return True

        hash_value = base64.b64encode(hash_function(data).digest()).decode()
        return hash_value == expected_hash and len(data) == expected_size

    def save_file(self, data, target, filename, filesize, download_start) -> None:
        try:
            target.write_bytes(data)
            download_time = (dt.now() - download_start).total_seconds()
            LOGGER.info("Downloaded %s of size %d bytes in %.2f seconds", filename, filesize, download_time)
        except Exception as e:
            LOGGER.error("Error saving to disk: %s", target)
            LOGGER.error(e)
