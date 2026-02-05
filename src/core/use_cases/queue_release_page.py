import logging

from src.core.entities.release import Release
from src.core.entities.release_page_queue_body import ReleasePageQueueBody
from src.core.interfaces.parser import ParserProvider
from src.core.interfaces.queue import QueueProvider
from src.core.interfaces.repository import RepositoryProvider
from src.core.interfaces.storage import StorageProvider

logger = logging.getLogger(__name__)


class QueueReleasePage:
    def __init__(self,
                 storage: StorageProvider,
                 parser: ParserProvider,
                 repository: RepositoryProvider,
                 queue: QueueProvider):
        self.storage = storage
        self.parser = parser
        self.queue = queue
        self.repository = repository

    def run(self, release: Release, page_num: int):
        try:
            message_body = ReleasePageQueueBody(
                release=release,
                page_num=page_num,
            )
            self.queue.send_data(message_body)
            logger.debug(f"Queued page {page_num} for "
                         f"release {release.filename}")

        except Exception as e:
            logger.error(f"Failed to queue page {page_num} "
                         f"for release {release.filename}: {e}",
                         exc_info=True)
            return
