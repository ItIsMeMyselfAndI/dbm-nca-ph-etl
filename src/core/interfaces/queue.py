from typing import Protocol

from src.core.entities.queue_release_page_body import QueueReleasePageBody
from src.core.entities.release import Release


class QueueProvider(Protocol):
    def send_data(self, data: Release | QueueReleasePageBody) -> None:
        """sends a batch of extracted rows to the queue"""
        ...
