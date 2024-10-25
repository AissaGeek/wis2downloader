"""
This module provides an abstract base class for storage and a concrete
implementation for S3 storage.
"""

from abc import abstractmethod, ABC
from pathlib import Path
from typing import BinaryIO

import minio

from wis2downloader.log import LOGGER


class Storage(ABC):
    """
    Abstract base class for storage implementations.
    """

    # @abstractmethod
    def __init__(self, defs, **kwargs):
        self.type = defs.get('type')
        self.options = defs.get('options')

    @abstractmethod
    def exists(self, filename: Path) -> bool:
        """
        Verify whether data already exists

        :param filename: `Path` of storage object/file

        :returns: `bool` of whether the filepath exists in storage
        """

    @abstractmethod
    def save(self, data: bytes, filename: Path) -> bool:
        """
        Save data to storage

        :param data: `bytes` of data
        :param filename: `str` of filename
        :param content_type: media type (default is `application/octet-stream`)

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

    def __init__(self, defs, **kwargs):
        super().__init__(defs, **kwargs)
        self._s3_bucket = self.options['bucket']
        self._client = minio.Minio(endpoint=self.options['url'], **kwargs)

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


class FileSystem(Storage):
    """
    TODO handle exceptions
    Concrete implementation of the Storage class for a local file system.
    Provides methods to interact with the file system for storage operations.
    """

    def __init__(self, defs):
        super().__init__(defs)
        self._base_path = Path(self.options['base_path'])

    def exists(self, filename: Path) -> bool:
        return filename.exists()

    def save(self, data: BinaryIO, filename: Path) -> bool:
        try:
            file_path = self._base_path / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if self.exists(file_path):
                LOGGER.warning('File already exists: %s', file_path)
                return False
            with open(file_path, 'wb') as file:
                file.write(data.read())
            LOGGER.info('Data saved to %s', file_path)
            return True
        except Exception as err:
            LOGGER.error("Error saving file: %s", err)
            return False
