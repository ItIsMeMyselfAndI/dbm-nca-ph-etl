from typing import Protocol
from io import BytesIO


class StorageProvider(Protocol):

    def get_base_storage_path(self) -> str:
        """return the base storage path as string"""
        ...

    def save_file(self, storage_path: str, data: BytesIO):
        """saves data to the destination (s3, disk, etc)"""
        ...

    def load_file(self, storage_path: str) -> BytesIO:
        """load data into memory"""
        ...
