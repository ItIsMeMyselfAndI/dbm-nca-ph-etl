import logging

from src.core.entities.release import Release
from src.core.entities.release_page_queue_body import ReleasePageQueueBody
from src.core.interfaces.queue import QueueProvider


logger = logging.getLogger(__name__)


class MockQueue(QueueProvider):
    def __init__(self):
        pass

    def send_data(self, data: Release | ReleasePageQueueBody) -> None:
        print({"message": data})
