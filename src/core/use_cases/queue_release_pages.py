import logging

from src.core.entities.queue_release_page_body import QueueReleasePageBody
from src.core.entities.release import Release
from src.core.interfaces.parser import ParserProvider
from src.core.interfaces.queue import QueueProvider
from src.core.interfaces.repository import RepositoryProvider
from src.core.interfaces.storage import StorageProvider

logger = logging.getLogger(__name__)


class QueueReleasePages:
    def __init__(self,
                 storage: StorageProvider,
                 parser: ParserProvider,
                 repository: RepositoryProvider,
                 queue: QueueProvider):
        self.storage = storage
        self.parser = parser
        self.queue = queue
        self.repository = repository

    def run(self, release: Release) -> str | None:
        storage_path = (f"{self.storage.get_base_storage_path()}/"
                        f"{release.filename}")
        for i in range(release.page_count):
            try:
                message_body = QueueReleasePageBody(
                    release=release,
                    page_num=i,
                )
                self.queue.send_data(message_body)

            except Exception as e:
                logger.error(f"Failed to queue page {i} for "
                             f"release {release.filename}: {e}",
                             exc_info=True)
                continue

            # <test>
            if i == 1:
                break
            # </test>

        return storage_path

    def _get_page_count(self, filename: str) -> int:
        data = self.storage.load_file(filename)
        page_count = self.parser.get_page_count(data)
        return page_count
