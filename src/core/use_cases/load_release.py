import logging
from src.core.entities.page_raw_table import PageRawTable
from src.core.interfaces.data_cleaner import DataCleanerProvider
from src.core.interfaces.repository import RepositoryProvider

logger = logging.getLogger(__name__)


class LoadRelease:
    def __init__(self,
                 repository: RepositoryProvider,
                 data_cleaner: DataCleanerProvider):
        self.data_cleaner = data_cleaner
        self.repository = repository

    def run(self, page_table: PageRawTable):
        try:
            logger.debug(f"Loading {page_table.release.filename} data to db: "
                         f"page-{page_table.page_num}...")
            self.repository.upsert_release(page_table.release)

            page_data = self.data_cleaner.clean_raw_data(page_table.rows,
                                                         page_table.release.id)

            self.repository.bulk_upsert_records(page_data.records)
            self.repository.bulk_insert_allocations(page_data.allocations)
            logger.debug(f"Loaded {page_table.release.filename} data to db: "
                         f"page-{page_table.page_num}")

        except Exception as e:
            logger.warning(f"Skipped loading {page_table.release.filename} "
                           f"(page-{page_table.page_num}): {e}")
