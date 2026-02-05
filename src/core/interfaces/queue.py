from typing import Protocol

from src.core.entities.release import Release
from src.core.entities.release_page_queue_body import ReleasePageQueueBody


class QueueProvider(Protocol):
    def send_data(self, data: Release | ReleasePageQueueBody) -> None:
        """sends a batch of extracted rows to the queue"""
        ...
