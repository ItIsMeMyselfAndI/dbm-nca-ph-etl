import os
from io import BytesIO

from src.core.interfaces.storage import StorageProvider


class LocalStorage(StorageProvider):
    def __init__(self, base_storage_path: str):
        self.base_storage_path = base_storage_path
        self._create_base_dirs()

    def get_filename_full_path(self, filename: str) -> str:
        if self.base_storage_path is None or self.base_storage_path == '':
            return filename
        if self.base_storage_path.endswith('/'):
            return f"{self.base_storage_path}{filename}"
        return f"{self.base_storage_path}/{filename}"

    def save_file(self, filename: str, data: BytesIO) -> None:
        self._create_base_dirs()
        data.seek(0)
        full_path = self.get_filename_full_path(filename)
        with open(full_path, 'wb') as f:
            f.write(data.read())

    def load_file(self, filename: str) -> BytesIO:
        full_path = self.get_filename_full_path(filename)
        with open(full_path, 'rb') as f:
            return BytesIO(f.read())

    def _create_base_dirs(self):
        if not self.base_storage_path:
            return
        os.makedirs(self.base_storage_path, exist_ok=True)
