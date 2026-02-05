import json
import boto3
import logging

from src.core.entities.release import Release
from src.core.entities.release_page_queue_body import ReleasePageQueueBody
from src.core.interfaces.queue import QueueProvider

logger = logging.getLogger(__name__)


class SQSQueue(QueueProvider):
    def __init__(self, queue_url: str):
        self.sqs = boto3.client('sqs')
        self.queue_url = queue_url

    def send_data(self, data: Release | ReleasePageQueueBody) -> None:
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
