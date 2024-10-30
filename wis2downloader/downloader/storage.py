"""
This module provides an abstract base class for storage and a concrete
implementation for S3 storage.
"""

import enum
import io
import re
import shutil
from abc import abstractmethod, ABC
from datetime import datetime as dt
from pathlib import Path

import minio
from minio import S3Error
from tenacity import retry, stop_after_attempt, wait_fixed, before_log, after_log
from urllib3 import PoolManager
from urllib3.exceptions import HTTPError

from wis2downloader.log import LOGGER

__all__ = ['S3', 'FS', 'StorageKeyType', 'ALL_STORAGES']

ALL_STORAGES = ['S3', 'FS']


class Storage(ABC):
    """
    Abstract base class for storage implementations.
    """

    @abstractmethod
    def exists(self, key: Path) -> bool:
        """
        Verify whether data already exists

        :param key: `Path` of storage object/file

        :returns: `bool` of whether the filepath exists in storage
        """

    @abstractmethod
    def save(self, data, key, file_size, content_type) -> bool:
        """
        Save data to storage
        :param data: `bytes` of data
        :param key: `str` of key
        :param file_size: `int` of file_size (default is -1)
        :param content_type: `str` of content type (default is `application/octet-stream`)

        :returns: `bool` of save result
        """


class S3(Storage):
    """
    Concrete implementation of the Storage class for AWS S3.
    Provides methods to interact with S3 for storage operations.
    """
    __name__ = 'S3'
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(S3, cls).__new__(cls)
        return cls._instance

    def __init__(self, **kwargs):
        if not hasattr(self, '_initialized'):
            self._s3_bucket = kwargs.pop('bucket')
            http_client = PoolManager(
                retries=0,
                timeout=5
            )
            self._client = minio.Minio(http_client=http_client, secure=False, **kwargs)
            self._initialized = True

        # Check connection to Minio
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_fixed(1),
            before=before_log(LOGGER, 20),
            after=after_log(LOGGER, 20),
        )
        def _check_connection():
            try:
                all_buckets = self._client.list_buckets()
                LOGGER.info(
                    "Connection to Minio established. Buckets available are : %s ", all_buckets)
            except Exception as e:
                LOGGER.error("Cannot establish connection to Minio. REASON: %s", str(e))
                raise e

        _check_connection()

    def exists(self, key: str) -> bool:
        try:
            self._client.stat_object(self._s3_bucket, key)
        except S3Error as e:
            if "NoSuchKey" in e.code:
                return False
            raise e
        return True

    def save(self, data: bytes, key: str = '',
             file_size: int = -1, content_type: str = 'application/octet-stream') -> bool:
        try:
            if self.exists(key):
                LOGGER.warning('Object already exists: %s', key)
                return False
            self._client.put_object(
                self._s3_bucket,
                key,
                io.BytesIO(data),
                length=-1,
                part_size=10 * 1024 * 1024,
                content_type=content_type
            )
            LOGGER.info('Data saved on S3 to %s', key)
            return True
        except minio.error.S3Error as err:
            LOGGER.error("Error saving file on S3: %s", err)
            return False
        except HTTPError as err:
            LOGGER.error("Error saving file on S3: %s", err)
            return False


class FS(Storage):
    """
    Concrete implementation of the Storage class for a local file system.
    Provides methods to interact with the file system for storage operations.
    """
    __name__ = 'FS'
    _instance = None
    min_free_space: int = 10  # GBytes

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(FS, cls).__new__(cls)
        return cls._instance

    def __init__(self, **kwargs):
        if not hasattr(self, '_initialized'):
            self._base_path: str = kwargs['basedir'].strip()
            self.min_free_space = self.min_free_space * (1024 ** 3)  # GBytes to Bytes
            self._initialized = True

    def exists(self, key: Path) -> bool:
        return key.exists()

    def save(
            self,
            data: bytes,
            key: str,
            file_size: int = -1,
            _: str = '') -> bool:
        # TODO CONFIG['min_free_space']
        try:
            file_path = Path(self._base_path) / key
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if self.exists(file_path):
                LOGGER.warning('File already exists: %s', file_path)
                return False
            if self.min_free_space > 0:
                free_space = self.get_free_space()
                if free_space < self.min_free_space:
                    LOGGER.warning("Too little free space, %d < %d, file %s not saved",
                                   free_space - file_size, self.min_free_space, key)
                    return False
            with open(file_path, 'wb') as file:
                file.write(data)
            LOGGER.info('Data saved on FS to %s', file_path)
            return True
        except (OSError, IOError) as err:
            LOGGER.error("Error saving file on FS: %s", err)
            return False

    def get_free_space(self):
        """
        Get the free disk space available at the base path.

        :returns: Free space in bytes.
        """
        _, _, free = shutil.disk_usage(self._base_path)
        return free


def _extract_relative_topic(topic: str) -> str:
    """
    Extract relative topic from full global broker topic
    :param topic:
    :return:
    """
    pattern = r'a/wis2/(.*)'
    match = re.search(pattern, topic)
    if match:
        return match.group(1)
    return ''


def get_date_now() -> str:
    """
    Returns today's date in the format yyyy/mm/dd.
    """
    today = dt.now()
    return f'{today.strftime("%Y")}/{today.strftime("%m")}/{today.strftime("%d")}'


class StorageKeyType(enum.Enum):
    """
    Enum for different types of storage backends.
    """
    S3 = "S3"
    FS = "FS"

    def build_key(self, job: dict, file_name: str) -> str:
        """
        Build a storage key based on the storage type.

        :param job: information to build base key oath
        :param file_name: The file_name with extension .
        :returns: A string representing the storage key.
        """

        if self.name == 'S3':
            base_key_s3 = _extract_relative_topic(job.get("topic"))
            return f"{base_key_s3}/{file_name}"
        elif self.name == 'FS':
            base_key_fs = Path(get_date_now()) / job.get("target", ".")
            return str(base_key_fs / file_name)
        else:
            raise KeyError(
                "No such storage"
            )
