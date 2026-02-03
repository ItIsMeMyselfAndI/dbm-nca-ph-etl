import logging

from src.core.entities.raw_page_table import RawPageTable
from src.core.interfaces.parser import ParserProvider
from src.core.interfaces.storage import StorageProvider

logger = logging.getLogger(__name__)


class ExtractPageTable:
    def __init__(self, storage: StorageProvider, parser: ParserProvider):
        self.storage = storage
        self.parser = parser

    def run(self, filename: str, page_num: int) -> RawPageTable | None:

        try:
            page_table = self._extract_tables(filename, page_num)
            return page_table

        except Exception as e:
            logger.error(f"Failed to extract {filename} "
                         f"page-{page_num}: {e}")
            return None

    def _extract_tables(self, filename: str,
                        page_num: int) -> RawPageTable | None:
        logger.debug(f"Extracting raw data from {filename}: "
                     f"page-{page_num}...")

        raw_data = self.storage.load_file(filename)

        rows = self.parser.extract_table_by_page_num(raw_data, page_num)
        raw_table = RawPageTable(
            rows=rows,
            page_num=page_num
        )

        logger.debug(f"Extraction complete for page-{page_num}")
        return raw_table
