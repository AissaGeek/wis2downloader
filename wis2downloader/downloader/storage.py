"""
This module provides an abstract base class for storage and a concrete
implementation for S3 storage.
"""

import io
import shutil
from abc import abstractmethod, ABC
from datetime import datetime as dt
from pathlib import Path

import minio
from urllib3 import PoolManager
from urllib3.exceptions import HTTPError

from wis2downloader.log import LOGGER

__all__ = ['S3', 'FS']


def get_date_now() -> str:
    """
    Returns today's date in the format yyyy/mm/dd.
    """
    today = dt.now()
    return f'{today.strftime("%Y")}/{today.strftime("%m")}/{today.strftime("%d")}'


class Storage(ABC):
    """
    Abstract base class for storage implementations.
    """

    @abstractmethod
    def exists(self, filename: Path) -> bool:
        """
        Verify whether data already exists

        :param filename: `Path` of storage object/file

        :returns: `bool` of whether the filepath exists in storage
        """

    @abstractmethod
    def save(self, data, filename, filesize, content_type) -> bool:
        """
        Save data to storage
        :param data: `bytes` of data
        :param filename: `str` of filename
        :param filesize: `int` of filesize (default is -1)
        :param content_type: `str` of content type (default is `application/octet-stream`)

        :returns: `bool` of save result
        """


# def _metric_save(func):
#     """
#     Decorator to increment metrics for successful saves
#     """
#     def decorator(*args, **kwargs):
#         def wrapper(target):
#             result = func(*args, **kwargs)
#             if result:
#                 DOWNLOADED_FILES.labels(target=args[0].target, topic=args[0].topic, centre_id=args[0].centre_id, file_type=args[0].file_type).inc(1)
#                 DOWNLOADED_BYTES.labels(target=args[0].target, topic=args[0].topic, centre_id=args[0].centre_id, file_type=args[0].file_type).inc(args[1])
#             return result
#     return wrapper

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
            self._client = minio.Minio(**kwargs, http_client=http_client, secure=False)
            self._initialized = True
            self._s3_path = ""

    @property
    def s3_path(self) -> str:
        return self._s3_path

    @s3_path.setter
    def s3_path(self, path: str):
        self._s3_path += path.strip()

    def exists(self, filename: str) -> bool:
        self._client.stat_object(self._s3_bucket, str(filename))
        return True

    def save(self, data: bytes, filename: str,
             _: int = -1, content_type: str = 'application/octet-stream') -> bool:
        try:
            if self.exists(filename):
                LOGGER.warning('Object already exists: %s', filename)
                return False
            self._client.put_object(
                self._s3_bucket,
                str(filename),
                io.BytesIO(data),
                length=-1,
                part_size=10 * 1024 * 1024,
                content_type=content_type
            )
            LOGGER.info('Data saved to %s', filename)
            return True
        except minio.error.S3Error as err:
            LOGGER.error("Error saving file: %s", err)
            return False
        except HTTPError as err:
            LOGGER.error("Error saving file: %s", err)
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
            self._original_path: str = kwargs['basedir'].strip()
            self.min_free_space = self.min_free_space * (1024 ** 3)  # GBytes to Bytes
            self._initialized = True
            self._base_path = ""

    @property
    def base_path(self) -> str:
        return self._base_path

    @base_path.setter
    def base_path(self, path: str):
        self._base_path = self._original_path
        self._base_path += f"/{get_date_now()}/{path.strip()}"

    def exists(self, filename: Path) -> bool:
        return filename.exists()

    def save(self, data: bytes, filename: str, filesize: int = -1, _: str = '') -> bool:
        # TODO CONFIG['min_free_space']
        try:
            file_path = Path(self._base_path) / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if self.exists(file_path):
                LOGGER.warning('File already exists: %s', file_path)
                return False
            if self.min_free_space > 0:
                free_space = self.get_free_space()
                if free_space < self.min_free_space:
                    LOGGER.warning("Too little free space, %d < %d, file %s not saved",
                                   free_space - filesize, self.min_free_space, filename)
                    return False
            with open(file_path, 'wb') as file:
                file.write(data)
            LOGGER.info('Data saved to %s', file_path)
            return True
        except (OSError, IOError) as err:
            LOGGER.error("Error saving file: %s", err)
            return False

    def get_free_space(self):
        """
        Get the free disk space available at the base path.

        :returns: Free space in bytes.
        """
        _, _, free = shutil.disk_usage(self._base_path)
        return free
