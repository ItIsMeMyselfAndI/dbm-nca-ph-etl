from typing import Protocol
from io import BytesIO


class StorageProvider(Protocol):

    def get_filename_full_path(self, filename: str) -> str:
        """return the full storage path of a file"""
        ...

    def save_file(self, filename: str, data: BytesIO):
        """saves data to the destination (s3, disk, etc)"""
        ...

    def load_file(self, filename: str) -> BytesIO:
        """load data into memory"""
        ...
