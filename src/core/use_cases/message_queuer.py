import logging

from pydantic import BaseModel

from src.core.interfaces.queue import QueueProvider

logger = logging.getLogger(__name__)


class MessageQueuer:
    def __init__(self, queue: QueueProvider):
        self.queue = queue

    def run(self, message: BaseModel) -> bool:
        try:
            logger.debug(f"Queueing message: {message}")
            self.queue.send(message)
            logger.debug(f"Successfully queued message: {message}")
            return True

        except Exception as e:
            logger.error(
                f"Failed to queue message: {message}\n" f">> {e}", exc_info=True
            )
            return False
