from io import BytesIO
import logging
from math import ceil
from typing import List

from tqdm import tqdm
from src.core.entities.nca_raw_table import NCARawTable
from src.core.entities.release import Release
from src.core.interfaces.parser import ParserProvider
from src.core.interfaces.storage import StorageProvider

logger = logging.getLogger(__name__)


class ExtractRelease:
    def __init__(self,
                 storage: StorageProvider,
                 parser: ParserProvider,
                 batch_size: int,):
        self.storage = storage
        self.parser = parser
        self.batch_size = batch_size

    def run(self, release: Release) -> List[NCARawTable]:

        try:
            logger.info(f"Extracting raw data from {release.filename}...")
            storage_path = (f"{self.storage.get_base_storage_path()}/"
                            f"{release.filename}")
            raw_data = self.storage.load_file(storage_path)
            page_data_list = self.parser.split_pages(raw_data)
            pages = self._extract_tables(release, page_data_list)
            table_batches = self._group_tables(release, pages)
            logger.info(f"Extraction complete for {release.filename}.")
            return table_batches

        except Exception as e:
            logger.error(f"Failed to extract {release.filename} data: {e}")
            return []

    def _extract_tables(self, release: Release, page_data_list: List[BytesIO]):
        page_count = len(page_data_list)
        pages = []
        page_success_count = 0

        for i in tqdm(range(page_count), desc="Extracting tables", unit="table"):
            try:
                page_table = self.parser.extract_tables(page_data_list[i])
                pages.append(page_table)
                page_success_count += 1
                # <test>
                # break
                # </test>

            except Exception as e:
                logger.warning(f"Skipped extracting {release.filename} "
                               f"(page-{i}): {e}")
                continue

        logger.info(
            f"Extracted {page_success_count}/{page_count} successfully.")

        return pages

    def _group_tables(self, release: Release, pages: List):
        table_batches: List[NCARawTable] = []
        total_batch_count = ceil(len(pages) / self.batch_size)
        success_count = 0

        for i in tqdm(range(total_batch_count), desc="Grouping tables", unit="table"):
            try:
                grouped_pages = pages[i * self.batch_size:
                                      (i + 1) * self.batch_size]
                joined_table = [row for page in grouped_pages
                                for row in page]
                nca_table = NCARawTable(
                    release=release.model_dump(),  # pyright: ignore
                    rows=joined_table,
                    batch_number=i
                )
                table_batches.append(nca_table)
                success_count += 1

            except Exception as e:
                logger.warning(f"Skipped grouping batch-{i}: {e}")
                continue

        logger.info(f"Grouped {success_count}/"
                    f"{total_batch_count} successfully.")

        return table_batches
