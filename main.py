import time
import logging
import sys
from tqdm import tqdm
from datetime import timedelta
from src.core.use_cases.load_allocations_and_records_to_db import (
    LoadRecordsAndAllocationsToDB,
)
from src.core.use_cases.queue_release_page import QueueReleasePage
from src.core.use_cases.scrape_and_queue_releases import ScrapeAndQueueReleases
from src.infrastructure.adapters.s3_storage import S3Storage
from src.logging_config import setup_logging

from src.core.use_cases.extract_page_table import ExtractPageTable
from src.infrastructure.adapters.mock_queue import MockQueue
from src.infrastructure.adapters.supabase_repository import SupabaseRepository
from src.infrastructure.adapters.local_storage import LocalStorage
from src.infrastructure.adapters.nca_scraper import NCAScraper
from src.infrastructure.adapters.pd_data_cleaner import PdDataCleaner
from src.infrastructure.adapters.pdf_parser import PDFParser
from src.infrastructure.constants import (
    ALLOCATION_COLUMNS,
    BASE_STORAGE_PATH,
    DB_BULK_SIZE,
    RECORD_COLUMNS,
    VALID_COLUMNS,
)

# <test>
MAX_PAGE_COUNT_TO_PUBLISH = None
# </test>


setup_logging()
logger = logging.getLogger(__name__)

# adapters
scraper = NCAScraper()
# storage = LocalStorage(base_storage_path=BASE_STORAGE_PATH)
storage = S3Storage(base_storage_path=BASE_STORAGE_PATH)
parser = PDFParser()
queue = MockQueue()
data_cleaner = PdDataCleaner(
    allocation_comumns=ALLOCATION_COLUMNS,
    record_columns=RECORD_COLUMNS,
    valid_columns=VALID_COLUMNS,
)
repository = SupabaseRepository(db_bulk_size=DB_BULK_SIZE)

# use cases
scrape_and_queue_job = ScrapeAndQueueReleases(
    scraper=scraper,
    storage=storage,
    parser=parser,
    queue=queue,
    repository=repository,
)
queue_job = QueueReleasePage(queue=queue)
extract_job = ExtractPageTable(storage=storage, parser=parser)
clean_job = PdDataCleaner(
    allocation_comumns=ALLOCATION_COLUMNS,
    record_columns=RECORD_COLUMNS,
    valid_columns=VALID_COLUMNS,
)
load_job = LoadRecordsAndAllocationsToDB(
    data_cleaner=data_cleaner,
    repository=repository,
)


def main():
    logger.info("Initializing NCA Pipeline...")

    try:
        logger.info("Starting Scraping Job...")
        releases = scrape_and_queue_job.run(oldest_release_year=2024)
        logger.info("Job completed successfully.")

        logger.info("Starting Processing Job...")
        for release in releases:
            logger.info(
                f"Processing & Loading " f"{release.filename} raw data to db..."
            )

            # <test>
            if MAX_PAGE_COUNT_TO_PUBLISH is not None:
                page_count = min(release.page_count, MAX_PAGE_COUNT_TO_PUBLISH)
            else:
                page_count = release.page_count
            # </test>

            prev_time = time.time()
            for page_num in tqdm(
                range(page_count), desc="Processing/Loading", unit="file"
            ):
                # queue
                queue_job.run(release, page_num)

                # extract
                extracted_table = extract_job.run(release.filename, page_num)

                if not extracted_table:
                    logger.warning(
                        f"Skipped: No table extracted for release "
                        f"{release.id} page-{page_num}"
                    )
                    continue

                # clean
                cleaned_table = clean_job.clean_raw_data(
                    extracted_table.rows, release.id
                )
                # load
                load_job.run(release, cleaned_table, page_num)

            elapsed = str(timedelta(seconds=time.time() - prev_time)).split(":")
            logger.info(
                f"Processing & Loading complete in "
                f"{elapsed[0]}h "
                f"{elapsed[1]}m "
                f"{elapsed[2]}s: "
                f"{release.filename}"
            )
        logger.info("Job completed successfully.")

    except Exception as e:
        logger.critical(f"Job crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
