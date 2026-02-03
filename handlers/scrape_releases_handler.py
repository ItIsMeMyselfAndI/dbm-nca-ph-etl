import logging

from src.infrastructure.adapters.sqs_queue import SQSQueue
from src.infrastructure.config import settings
from src.logging_config import setup_logging

from src.core.use_cases.scrape_releases import ScrapeReleases
from src.infrastructure.adapters.s3_storage import S3Storage
from src.infrastructure.adapters.nca_scraper import NCAScraper
from src.infrastructure.adapters.pdf_parser import PDFParser
from src.infrastructure.adapters.supabase_repository import SupabaseRepository
from src.infrastructure.constants import (BASE_STORAGE_PATH,
                                          DB_BULK_SIZE)

setup_logging()
logger = logging.getLogger(__name__)

# adapters
scraper = NCAScraper()
storage = S3Storage(bucket_name=settings.AWS_S3_BUCKET_NAME,
                    base_storage_path=BASE_STORAGE_PATH)
parser = PDFParser()
queue = SQSQueue(queue_url=settings.AWS_SQS_RELEASE_QUEUE_URL)
repository = SupabaseRepository(db_bulk_size=DB_BULK_SIZE)

# use cases
scrape_job = ScrapeReleases(scraper=scraper,
                            storage=storage,
                            parser=parser,
                            queue=queue,
                            repository=repository,)


def lambda_handler(event, context):
    logger.info("Starting Scraping Job...")
    try:
        releases = scrape_job.run(oldest_release_year=2024)

    except Exception as e:
        logger.critical(f"Scraping job failed: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

    logger.info("Job completed successfully.")
    return {"status": "success", "releases_processed": len(releases)}
