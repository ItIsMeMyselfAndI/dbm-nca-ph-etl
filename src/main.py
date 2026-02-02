from src.core.use_cases.extract_release import ExtractRelease
from src.core.use_cases.load_release import LoadRelease
from src.core.use_cases.scrape_releases import ScrapeReleases
from src.core.use_cases.queue_data import QeueuData
from src.infrastructure.adapters.sqs_queue import SQSQueue
from src.infrastructure.adapters.supabase_repository import SupabaseRepository
from src.infrastructure.adapters.local_storage import LocalStorage
from src.infrastructure.adapters.nca_scraper import NCAScraper
import logging
import sys

from src.infrastructure.adapters.pd_data_cleaner import PdDataCleaner
from src.infrastructure.adapters.pdf_parser import PDFParser
from src.infrastructure.constants import (ALLOCATION_COLUMNS,
                                          BASE_STORAGE_PATH,
                                          DB_BULK_SIZE,
                                          RECORD_COLUMNS,
                                          VALID_COLUMNS)
from src.infrastructure.logging_config import setup_logging


setup_logging()
logger = logging.getLogger(__name__)


def main():
    logger = logging.getLogger("main")
    logger.info("Initializing NCA Pipeline...")

    # init adapters
    logger.debug("Setting up adapters...")
    scraper = NCAScraper()
    storage = LocalStorage(base_storage_path=BASE_STORAGE_PATH)
    parser = PDFParser()
    queue = SQSQueue()
    data_cleaner = PdDataCleaner(allocation_comumns=ALLOCATION_COLUMNS,
                                 record_columns=RECORD_COLUMNS,
                                 valid_columns=VALID_COLUMNS)
    repository = SupabaseRepository(db_bulk_size=DB_BULK_SIZE)

    # init use cases
    scrape_job = ScrapeReleases(scraper=scraper,
                                storage=storage,
                                parser=parser,
                                repository=repository,)
    extract_job = ExtractRelease(storage=storage,
                                 parser=parser,
                                 batch_size=DB_BULK_SIZE,)
    queue_job = QeueuData(queue=queue)
    load_job = LoadRelease(data_cleaner=data_cleaner,
                           repository=repository,)

    # excute
    try:
        logger.info("Starting Scraping Job...")
        releases = scrape_job.run(oldest_release_year=2024)
        logger.info("Job completed successfully.")

        logger.info("Starting Processing Job...")
        for release in releases:
            table_batches = extract_job.run(release)
            logger.info(f"Loading {release.filename} data to db...")
            for table in table_batches:
                queue_job.run(table)
                load_job.run(table)
                # <test>
                break
                # </test>
            logger.info(f"Loaded {release.filename}.")
        logger.info("Job completed successfully.")

    except Exception as e:
        logger.critical(f"Job crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
