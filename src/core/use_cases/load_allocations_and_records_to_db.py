import logging
from src.core.entities.nca_data import NCAData
from src.core.entities.release import Release
from src.core.interfaces.data_cleaner import DataCleanerProvider
from src.core.interfaces.repository import RepositoryProvider

logger = logging.getLogger(__name__)


class LoadRecordsAndAllocationsToDB:
    def __init__(self,
                 repository: RepositoryProvider,
                 data_cleaner: DataCleanerProvider):
        self.data_cleaner = data_cleaner
        self.repository = repository

    def run(self, release: Release, nca_data: NCAData, page_num: int):
        try:
            if len(nca_data.records) == 0:
                logger.warning(f"No records to load "
                               f"for {release.filename} "
                               f"(page-{page_num})")
                return
            self.repository.bulk_upsert_records(nca_data.records)

            if len(nca_data.allocations) == 0:
                logger.warning(f"No allocations to load "
                               f"for {release.filename} "
                               f"(page-{page_num})")
                return
            self.repository.bulk_insert_allocations(nca_data.allocations)

            logger.debug(f"Loaded {release.filename} data "
                         f"to db: page-{page_num}")

        except Exception as e:
            logger.warning(f"Skipped loading {release.filename} "
                           f"(page-{page_num}): {e}")
