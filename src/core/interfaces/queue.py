from typing import Protocol

from src.core.entities.page_raw_table import PageRawTable


class QueueProvider(Protocol):
    def send_data(self, data: PageRawTable) -> None:
        """sends a batch of extracted rows to the queue"""
        ...
