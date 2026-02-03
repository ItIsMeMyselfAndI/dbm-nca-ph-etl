import json
import boto3
import logging

from src.core.entities.queue_release_page_body import QueueReleasePageBody
from src.core.entities.release import Release
from src.core.interfaces.queue import QueueProvider
from src.infrastructure.config import settings

logger = logging.getLogger(__name__)


class SQSQueue(QueueProvider):
    def __init__(self, queue_url: str):
        self.sqs = boto3.client(
            'sqs',
            region_name=settings.AWS_REGION,
            # aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            # aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        self.queue_url = queue_url

    def send_data(self, data: Release | QueueReleasePageBody) -> None:
        try:
            message_body = json.dumps(data.model_dump(mode='json'))

            self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message_body
            )

            logger.debug(f"Sent data to queue: {data}")

        except Exception as e:
            logger.error(f"Failed to send data to SQS: {e}")
            raise e
