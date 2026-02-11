from functools import lru_cache
import logging

from src.core.interfaces.storage import StorageProvider

logger = logging.getLogger(__name__)


class FileBytesMemoLoader:
    def __init__(self, storage: StorageProvider):
        self.storage = storage

    @lru_cache(maxsize=1)
    def run(self, filename: str) -> bytes | None:
        try:
            logger.info(f"Loading file stream to memory for {filename}...")
            file_stream = self.storage.load_file(filename)
            if not file_stream:
                logger.warning(f"No file stream found for {filename}")
                return None
            logger.info(f"Loaded file stream to memory for {filename}")
            return file_stream.read()

        except Exception as e:
            logger.error(f"Error loading file stream memo for {filename}: {e}")
            return None
