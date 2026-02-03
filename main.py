import time
import logging
import sys
from tqdm import tqdm
from datetime import timedelta
from src.infrastructure.adapters.s3_storage import S3Storage
from src.infrastructure.config import settings
from src.logging_config import setup_logging

from src.core.use_cases.extract_page_table import ExtractPageTable
from src.core.use_cases.load_release import LoadRelease
from src.core.use_cases.queue_release_pages import QueueReleasePages
from src.core.use_cases.scrape_releases import ScrapeReleases
from src.infrastructure.adapters.mock_queue import MockQueue
from src.infrastructure.adapters.supabase_repository import SupabaseRepository
from src.infrastructure.adapters.local_storage import LocalStorage
from src.infrastructure.adapters.nca_scraper import NCAScraper
from src.infrastructure.adapters.pd_data_cleaner import PdDataCleaner
from src.infrastructure.adapters.pdf_parser import PDFParser
from src.infrastructure.constants import (ALLOCATION_COLUMNS,
                                          BASE_STORAGE_PATH,
                                          DB_BULK_SIZE,
                                          RECORD_COLUMNS,
                                          VALID_COLUMNS)


setup_logging()
logger = logging.getLogger(__name__)

# adapters
scraper = NCAScraper()
storage = LocalStorage(base_storage_path=BASE_STORAGE_PATH)
parser = PDFParser()
queue = MockQueue()
data_cleaner = PdDataCleaner(allocation_comumns=ALLOCATION_COLUMNS,
                             record_columns=RECORD_COLUMNS,
                             valid_columns=VALID_COLUMNS)
repository = SupabaseRepository(db_bulk_size=DB_BULK_SIZE)

# use cases
scrape_job = ScrapeReleases(scraper=scraper,
                            storage=storage,
                            parser=parser,
                            queue=queue,
                            repository=repository,)
queue_release_job = QueueReleasePages(storage=storage,
                                      parser=parser,
                                      queue=queue,
                                      repository=repository)
extract_job = ExtractPageTable(storage=storage,
                               parser=parser)
load_job = LoadRelease(data_cleaner=data_cleaner,
                       repository=repository,)


def main():
    logger.info("Initializing NCA Pipeline...")

    # init adapters
    logger.debug("Setting up adapters...")
    # excute
    try:
        logger.info("Starting Scraping Job...")
        releases = scrape_job.run(oldest_release_year=2024)
        logger.info("Job completed successfully.")

        logger.info("Starting Processing Job...")
        for release in releases:
            logger.info(f"Extracting & Loading "
                        f"{release.filename} raw data to db...")

            # queue release pages
            storage_path = queue_release_job.run(release)

            if not storage_path:
                logger.warning(f"Skipped processing {release.filename}: "
                               f"Failed to queue release pages")
                continue

            # process
            prev_time = time.time()
            for page_num in tqdm(range(release.page_count),
                                 desc="Extracting/Loading", unit="file"):
                table = extract_job.run(storage_path, page_num)
                if not table:
                    logger.warning(f"Skipped extracting {release.filename} "
                                   f"page-{page_num}: Table Not Found")
                    continue
                load_job.run(release, table)
                # <test>
                break
                # </test>

            elapsed = str(timedelta(seconds=time.time() -
                                    prev_time)).split(":")
            logger.info(f"Loaded complete in "
                        f"{elapsed[0]}h "
                        f"{elapsed[1]}m "
                        f"{elapsed[2]}s: "
                        f"{release.filename}")
        logger.info("Job completed successfully.")

    except Exception as e:
        logger.critical(f"Job crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
