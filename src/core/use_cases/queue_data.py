from src.core.entities.nca_raw_table import NCARawTable
from src.core.interfaces.queue import QueueProvider


class QeueuData:
    def __init__(self, queue: QueueProvider):
        self.queue = queue
        pass

    def run(self, data: NCARawTable):
        self.queue.send_data(data)
