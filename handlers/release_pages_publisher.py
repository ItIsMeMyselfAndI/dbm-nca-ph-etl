from src.logging_config import setup_logging
from src.infrastructure.config import settings
from src.infrastructure.adapters.sqs_queue import SQSQueue
from src.core.use_cases.queue_release_page import QueueReleasePage
from src.core.entities.release import Release
import logging
import json

# <test>
MAX_PAGE_COUNT_TO_PUBLISH = None
# </test>


setup_logging()
logger = logging.getLogger(__name__)

# adapters
queue = SQSQueue(queue_url=settings.AWS_SQS_RELEASE_PAGE_QUEUE_URL)

# use case
queue_job = QueueReleasePage(queue=queue)


def lambda_handler(event, context):
    try:
        queued_releases = []

        for record in event['Records']:
            try:
                payload = record['body']
                if isinstance(payload, str):
                    payload = json.loads(payload)

                release = Release(**payload)

                queued_pages_count = 0
                total_pages_count = 0

                # <test>
                if MAX_PAGE_COUNT_TO_PUBLISH is not None:
                    page_count = min(release.page_count, MAX_PAGE_COUNT_TO_PUBLISH)
                else:
                    page_count = release.page_count
                # </test>

                for i in range(page_count):
                    try:
                        queue_job.run(release, i)
                        queued_pages_count += 1
                        total_pages_count += 1

                    except Exception as e:
                        logger.error(f"Failed to queue page {i} for "
                                     f"release {release.filename}: {e}",
                                     exc_info=True)
                        total_pages_count += 1
                        continue

                queued_releases.append(release.filename)
                logger.info(f"Successfully queued pages for "
                            f"release {release.id}: "
                            f"{queued_pages_count}/{total_pages_count}")

            except Exception as e:
                logger.error(f"Failed to process record: {e}", exc_info=True)
                continue

    except Exception as e:
        logger.critical(f"Release pages queuing job "
                        f"failed: {e}", exc_info=True)
        return {"statusCode": 400,
                "body": "Release pages queuing job failed."}

    return {
        'statusCode': 200,
        'body': ("Release pages queuing job completed successfully. "
                f"Queued releases: {queued_releases}")
    }
