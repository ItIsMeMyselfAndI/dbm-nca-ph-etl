import logging

from src.core.entities.queue_release_page_body import QueueReleasePageBody
from src.core.entities.release import Release
from src.core.interfaces.queue import QueueProvider


logger = logging.getLogger(__name__)


class MockQueue(QueueProvider):
    def __init__(self):
        pass

    def send_data(self, data: Release | QueueReleasePageBody) -> None:
        print({"message": data})
