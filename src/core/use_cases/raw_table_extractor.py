from io import BytesIO
import logging
from typing import List

from src.core.interfaces.parser import ParserProvider
from src.core.interfaces.storage import StorageProvider

logger = logging.getLogger(__name__)


class RawTableExtractor:
    def __init__(self, storage: StorageProvider, parser: ParserProvider):
        self.storage = storage
        self.parser = parser

    def run(self, data: BytesIO, page_num: int) -> List[List[str | None]] | None:
        try:
            logger.debug(f"Extracting raw table: page-{page_num}...")

            table = self.parser.extract_table_by_page_num(data, page_num)

            if len(table) == 0:
                logger.warning(f"No tables extracted from page-{page_num}")
                return None

            logger.debug(f"Extracted {len(table)} rows from page-{page_num}")
            return table

        except Exception as e:
            logger.error(
                f"Failed to extract tables from page-{page_num}: {e}",
                exc_info=True,
            )
            return None
