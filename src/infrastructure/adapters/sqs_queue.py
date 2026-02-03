import json
import boto3
import logging
from src.core.entities.page_raw_table import PageRawTable
from src.core.interfaces.queue import QueueProvider
from src.infrastructure.config import settings

logger = logging.getLogger(__name__)


class SQSQueue(QueueProvider):
    def __init__(self):
        self.sqs = boto3.client(
            'sqs',
            region_name=settings.AWS_REGION,
            # aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            # aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        self.queue_url = settings.SQS_QUEUE_URL

    def send_data(self, data: PageRawTable) -> None:
        # try:
        #     message_body = json.dumps(data.model_dump(mode='json'))
        #
        #     self.sqs.send_message(
        #         QueueUrl=self.queue_url,
        #         MessageBody=message_body
        #     )
        #
        #     logger.debug(f"Sent batch {data.batch_number} to queue.")
        #
        # except Exception as e:
        #     logger.error(f"Failed to send batch to SQS: {e}")
        #     raise e
        print({"message": data.release})
