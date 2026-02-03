import logging

from src.core.entities.page_raw_table import PageRawTable
from src.core.entities.release import Release
from src.core.interfaces.parser import ParserProvider
from src.core.interfaces.storage import StorageProvider

logger = logging.getLogger(__name__)


class ExtractPageTable:
    def __init__(self, storage: StorageProvider, parser: ParserProvider):
        self.storage = storage
        self.parser = parser

    def run(self, release: Release, page_num: int) -> PageRawTable | None:

        try:
            page_table = self._extract_tables(release, page_num)
            return page_table

        except Exception as e:
            logger.error(f"Failed to extract {release.filename} "
                         f"page-{page_num}: {e}")
            return None

    def _extract_tables(self, release: Release,
                        page_num: int) -> PageRawTable | None:
        logger.debug(f"Extracting raw data from {release.filename}: "
                     f"page-{page_num}...")

        storage_path = (f"{self.storage.get_base_storage_path()}/"
                        f"{release.filename}")

        raw_data = self.storage.load_file(storage_path)

        rows = self.parser.extract_table_by_page_num(raw_data, page_num)
        page_table = PageRawTable(
            release=release.model_dump(),  # pyright: ignore
            rows=rows,
            page_num=page_num
        )

        logger.debug(f"Extraction complete for page-{page_num}")
        return page_table
