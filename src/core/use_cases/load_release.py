import logging
from src.core.entities.raw_page_table import RawPageTable
from src.core.entities.release import Release
from src.core.interfaces.data_cleaner import DataCleanerProvider
from src.core.interfaces.repository import RepositoryProvider

logger = logging.getLogger(__name__)


class LoadRelease:
    def __init__(self,
                 repository: RepositoryProvider,
                 data_cleaner: DataCleanerProvider):
        self.data_cleaner = data_cleaner
        self.repository = repository

    def run(self, release: Release, raw_table: RawPageTable):
        try:
            logger.debug(f"Loading {release.filename} data to db: "
                         f"page-{raw_table.page_num}...")
            self.repository.upsert_release(release)

            raw_data = self.data_cleaner.clean_raw_data(raw_table.rows,
                                                        release.id)

            self.repository.bulk_upsert_records(raw_data.records)
            self.repository.bulk_insert_allocations(raw_data.allocations)
            logger.debug(f"Loaded {release.filename} data to db: "
                         f"page-{raw_table.page_num}")

        except Exception as e:
            logger.warning(f"Skipped loading {release.filename} "
                           f"(page-{raw_table.page_num}): {e}")
