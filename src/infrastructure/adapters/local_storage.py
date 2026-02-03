import os
from io import BytesIO

from src.core.interfaces.storage import StorageProvider


class LocalStorage(StorageProvider):
    def __init__(self, base_storage_path: str):
        self.abs_storage_path = os.path.abspath(base_storage_path)
        self._create_base_dirs()

    def get_base_storage_path(self):
        return self.abs_storage_path

    def save_file(self, storage_path: str, data: BytesIO) -> None:
        self._create_base_dirs()
        full_path = f"{self.abs_storage_path}/{storage_path}"
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        data.seek(0)
        with open(full_path, 'wb') as f:
            f.write(data.read())

    def load_file(self, storage_path: str) -> BytesIO:
        full_path = f"{self.abs_storage_path}/{storage_path}"
        with open(full_path, 'rb') as f:
            return BytesIO(f.read())

    def _create_base_dirs(self):
        os.makedirs(self.abs_storage_path, exist_ok=True)
