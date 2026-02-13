from io import BytesIO
import time
import logging
import sys
from tqdm import tqdm
from datetime import timedelta

from src.core.use_cases.file_stream_memo_loader import FileBytesMemoLoader
from src.core.use_cases.message_queuer import MessageQueuer
from src.core.use_cases.nca_db_loader import NCADBLoader
from src.core.use_cases.raw_table_cleaner import RawTableCleaner
from src.core.use_cases.raw_table_extractor import RawTableExtractor
from src.core.use_cases.release_batcher import ReleaseBatcher
from src.core.use_cases.releases_scraper import ReleasesScraper
from src.infrastructure.adapters.bs4_scraper import Bs4Scraper
from src.infrastructure.adapters.s3_storage import S3Storage
from src.infrastructure.adapters.scrapy_scraper import ScrapyScraper
from src.logging_config import setup_logging

from src.infrastructure.adapters.mock_queue import MockQueue
from src.infrastructure.adapters.supabase_repository import SupabaseRepository
from src.infrastructure.adapters.local_storage import LocalStorage
from src.infrastructure.adapters.pd_data_cleaner import PdDataCleaner
from src.infrastructure.adapters.pdf_parser import PDFParser
from src.infrastructure.constants import (
    ALLOCATION_COLUMNS,
    BASE_STORAGE_PATH,
    BATCH_SIZE,
    DB_BULK_SIZE,
    RECORD_COLUMNS,
    VALID_COLUMNS,
)

# <test>
NUMBER_OF_BATCHES_TO_QUEUE = 1
# </test>


setup_logging()
logger = logging.getLogger(__name__)

# adapters
scraper = Bs4Scraper()
# scraper = ScrapyScraper()
storage = LocalStorage(base_storage_path=BASE_STORAGE_PATH)
# storage = S3Storage(base_storage_path=BASE_STORAGE_PATH)
parser = PDFParser()
queue = MockQueue()
data_cleaner = PdDataCleaner(
    allocation_comumns=ALLOCATION_COLUMNS,
    record_columns=RECORD_COLUMNS,
    valid_columns=VALID_COLUMNS,
)
repository = SupabaseRepository(db_bulk_size=DB_BULK_SIZE)

# use cases
scraper_job = ReleasesScraper(
    scraper=scraper,
    parser=parser,
    storage=storage,
    repository=repository,
)
queuer_job = MessageQueuer(queue=queue)
batcher_job = ReleaseBatcher(batch_size=BATCH_SIZE)
file_bytes_loader_job = FileBytesMemoLoader(storage=storage)
extractor_job = RawTableExtractor(storage=storage, parser=parser)
cleaner_job = RawTableCleaner(data_cleaner=data_cleaner)
db_loader_job = NCADBLoader(
    data_cleaner=data_cleaner,
    repository=repository,
)


def main():
    logger.info("Initializing NCA Pipeline...")

    try:
        # ---------------------
        # scraper
        # ---------------------

        # scrape
        logger.info("Starting Scraping Job...")
        releases = scraper_job.run(oldest_release_year=2024)
        logger.info("Scraper Job completed successfully.")

        # queue
        logger.info("Starting Queueing Job...")
        success_count = 0
        for release in releases:
            is_queued = queuer_job.run(release)
            if is_queued:
                success_count += 1
        logger.info(f"Successfully queued {success_count}/{len(releases)} releases.")
        logger.info("Queuer completed successfully.")

        for release in releases:
            prev_time = time.time()

            # ---------------------
            # orchestrator
            # ---------------------

            # batcher
            logger.info("Starting batcher job...")
            batches = batcher_job.run(release)
            logger.info("Batcher job completed.")

            # <test>
            if NUMBER_OF_BATCHES_TO_QUEUE is not None:
                batch_count = min(NUMBER_OF_BATCHES_TO_QUEUE, len(batches))
                batches = batches[:batch_count]
                logger.info(f"Limiting to {batch_count} batches for testing purposes.")
            # </test>

            # queuer
            logger.info("Starting queuer job...")
            succcess_count = 0
            for batch in batches:
                is_queued = queuer_job.run(batch)
                if is_queued:
                    succcess_count += 1
            logger.info(
                f"Successfully queued {succcess_count}/{len(batches)} batches for "
                f"{release.filename}."
            )
            logger.info("Queuer job completed.")

            # ---------------------
            # worker
            # ---------------------

            logger.info("Starting Processing & Loading Job...")
            for batch in tqdm(
                batches, desc=f"Processing/Loading {release.filename}", unit="batch"
            ):
                # file bytes memo loader
                file_bytes = file_bytes_loader_job.run(batch.release.filename)
                if not file_bytes:
                    continue

                # extractor
                logger.debug(
                    f"Extracting {batch.release.filename} "
                    f"batch-{batch.batch_num} tables..."
                )
                extracted_table = []
                for i in range(batch.start_page_num, batch.end_page_num + 1):
                    table = extractor_job.run(BytesIO(file_bytes), i)
                    if not table:
                        logger.warning(
                            f"No tables extracted for {batch.release.filename} "
                            f"batch-{batch.batch_num} page-{i}"
                        )
                        continue
                    extracted_table.extend(table)

                if not extracted_table:
                    logger.warning(
                        f"No tables extracted for {batch.release.filename} "
                        f"batch-{batch.batch_num}"
                    )
                    continue
                logger.debug(
                    f"Extracted {len(extracted_table)} rows for "
                    f"{batch.release.filename} batch-{batch.batch_num}"
                )
                # cleaner
                logger.debug(
                    f"Cleaning {batch.release.id} batch-{batch.batch_num} tables..."
                )
                nca_data = cleaner_job.run(extracted_table, batch.release.id)
                logger.debug(
                    f"Cleaned data for {batch.release.filename} batch-{batch.batch_num}: "
                    f"{len(nca_data.allocations)} allocations, "
                    f"{len(nca_data.records)} records"
                )
                # loader
                logger.debug(
                    f"Loading {batch.release.id} batch-{batch.batch_num} data to db..."
                )
                db_loader_job.run(batch.release, nca_data, batch.batch_num)
                logger.debug(
                    f"Loaded {batch.release.filename} batch-{batch.batch_num} data to db"
                )

            elapsed = str(timedelta(seconds=time.time() - prev_time)).split(":")
            logger.info(
                f"Finished processing/loading {release.filename}: "
                f"{elapsed[0]}h "
                f"{elapsed[1]}m "
                f"{elapsed[2]}s: "
                f"{release.filename}"
            )
        logger.info("Jobs completed successfully.")

    except Exception as e:
        logger.critical(f"NCA Pipline crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
