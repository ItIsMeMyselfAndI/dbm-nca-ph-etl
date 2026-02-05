import json
import logging

from src.core.entities.release import Release
from src.core.use_cases.queue_release_page import QueueReleasePage
from src.infrastructure.adapters.sqs_queue import SQSQueue
from src.infrastructure.config import settings
from src.logging_config import setup_logging

from src.infrastructure.adapters.s3_storage import S3Storage
from src.infrastructure.adapters.pdf_parser import PDFParser
from src.infrastructure.adapters.supabase_repository import SupabaseRepository
from src.infrastructure.constants import (BASE_STORAGE_PATH,
                                          DB_BULK_SIZE)

setup_logging()
logger = logging.getLogger(__name__)

# adapters
storage = S3Storage(base_storage_path=BASE_STORAGE_PATH)
parser = PDFParser()
queue = SQSQueue(queue_url=settings.AWS_SQS_RELEASE_PAGE_QUEUE_URL)
repository = SupabaseRepository(db_bulk_size=DB_BULK_SIZE)

# use case
queue_pages_job = QueueReleasePage(storage=storage,
                                   parser=parser,
                                   queue=queue,
                                   repository=repository)


def lambda_handler(event, context):
    try:
        releases = []
        sucess_count = 0
        total_releases = len(event['Records'])

        for record in event['Records']:
            try:
                payload = record['body']
                if isinstance(payload, str):
                    payload = json.loads(payload)

                release = Release(**payload)

                for i in range(release.page_count):
                    queue_pages_job.run(release, i)

                    # <test>
                    if i > 5:
                        break
                    # </test>

                releases.append(release.filename)
                sucess_count += 1
                logger.info(f"Successfully queued pages for "
                            f"release: {release.filename}")

            except Exception as e:
                logger.error(f"Failed to process record: {e}", exc_info=True)
                continue

    except Exception as e:
        logger.critical(f"Release pages queuing job "
                        f"failed: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

    return {
        'statusCode': 200,
        'body': (f"Release pages queuing job "
                 f"completed successfully: "
                 f"{sucess_count}/{total_releases} releases processed.")
    }
