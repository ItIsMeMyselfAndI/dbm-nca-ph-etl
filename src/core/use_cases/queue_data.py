from src.core.entities.page_raw_table import PageRawTable
from src.core.interfaces.queue import QueueProvider


class QeueuData:
    def __init__(self, queue: QueueProvider):
        self.queue = queue
        pass

    def run(self, data: PageRawTable):
        self.queue.send_data(data)
