"""
This module provides an abstract base class for storage and a concrete
implementation for S3 storage.
"""

from abc import abstractmethod, ABC
from pathlib import Path
import shutil
from typing import BinaryIO
from datetime import datetime as dt

import minio

from wis2downloader.log import LOGGER

__all__ = ['S3', 'FS']


def get_date_now() -> str:
    """
    Returns today's date in the format yyyy/mm/dd.
    """
    today = dt.now()
    return f'{today.strftime("%Y")} / {today.strftime("%m")} / {today.strftime("%d")}'


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
    def save(self, data: bytes, filename: Path, filesize: int) -> bool:
        """
        Save data to storage

        :param data: `bytes` of data
        :param filename: `str` of filename
        :returns: `bool` of save result
        """


def true_or_false(func):
    """
    Decorator that wraps a function to handle exceptions from minio.
    Logs errors and returns False if an S3Error is raised.
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except minio.error.S3Error as err:
            LOGGER.error("S3 error: %s", err)
            return False
        except Exception as err:
            LOGGER.error("Unexpected error: %s", err)
            return False

    return wrapper


class S3(Storage):
    """
    Concrete implementation of the Storage class for AWS S3.
    Provides methods to interact with S3 for storage operations.
    """

    def __init__(self, **kwargs):
        self._s3_bucket = kwargs.pop('bucket')
        self._client = minio.Minio(**kwargs)

    @true_or_false
    def exists(self, filename: Path) -> bool:
        self._client.stat_object(self._s3_bucket, str(filename))
        return True

    @true_or_false
    def save(self, data: BinaryIO, filename: Path,
             content_type: str = 'application/octet-stream') -> bool:
        if self.exists(filename):
            LOGGER.warning('Object already exists: %s', filename)
            return False
        self._client.put_object(
            self._s3_bucket,
            str(filename),
            data,
            length=-1,
            part_size=10 * 1024 * 1024,
            content_type=content_type
        )
        LOGGER.info('Data saved to %s', filename)
        return True


class FS(Storage):
    """
    TODO handle exceptions
    Concrete implementation of the Storage class for a local file system.
    Provides methods to interact with the file system for storage operations.
    """
    min_free_space: int = 10  # GBytes

    def __init__(self, **kwargs):
        self._base_path = Path(kwargs['basedir']) / get_date_now()
        self.min_free_space = self.min_free_space * (1024 ** 3)  # GBytes to Bytes

    def exists(self, filename: Path) -> bool:
        return filename.exists()

    def save(self, data: BinaryIO, filename: Path, filesize: int) -> bool:
        try:
            file_path = self._base_path / filename
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
                file.write(data.read())
            LOGGER.info('Data saved to %s', file_path)
            return True
        except Exception as err:
            LOGGER.error("Error saving file: %s", err)
            return False

    def get_free_space(self):
        """
        Get the free disk space available at the base path.

        :returns: Free space in bytes.
        """
        _, _, free = shutil.disk_usage(self._base_path)
        return free
