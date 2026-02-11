import time
import logging
from datetime import timedelta

from src.logging_config import setup_logging
from src.core.use_cases.message_queuer import MessageQueuer
from src.core.use_cases.releases_scraper import ReleasesScraper
from src.infrastructure.adapters.s3_storage import S3Storage
from src.infrastructure.adapters.sqs_queue import SQSQueue
from src.infrastructure.config import settings

from src.infrastructure.adapters.supabase_repository import SupabaseRepository
from src.infrastructure.adapters.nca_scraper import NCAScraper
from src.infrastructure.adapters.pdf_parser import PDFParser
from src.infrastructure.constants import (
    BASE_STORAGE_PATH,
    DB_BULK_SIZE,
)

setup_logging()
logger = logging.getLogger(__name__)

# adapters
scraper = NCAScraper()
parser = PDFParser()
storage = S3Storage(base_storage_path=BASE_STORAGE_PATH)
repository = SupabaseRepository(db_bulk_size=DB_BULK_SIZE)
queue = SQSQueue(queue_url=settings.AWS_SQS_RELEASE_QUEUE_URL)

# use cases
scraper_job = ReleasesScraper(
    scraper=scraper,
    parser=parser,
    storage=storage,
    repository=repository,
)
queuer_job = MessageQueuer(queue=queue)


def lambda_handler(event, context):
    start_time = time.monotonic()

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

    end_time = time.monotonic()
    elapsed_time = timedelta(seconds=end_time - start_time)
    logger.info(f"Total elapsed time: {elapsed_time}")
