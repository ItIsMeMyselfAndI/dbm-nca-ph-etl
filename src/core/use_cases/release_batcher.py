from typing import List
import logging

from src.core.entities.release import Release
from src.core.entities.release_batch import ReleaseBatch

logger = logging.getLogger(__name__)


class ReleaseBatcher:
    def __init__(self, batch_size):
        self.batch_size = batch_size

    def run(self, release: Release) -> List[ReleaseBatch]:
        logger.info(
            f"Batching release: {release.filename}: {release.page_count} total pages"
        )

        batches = []
        total_batches = 0
        for start in range(1, release.page_count + 1, self.batch_size):
            total_batches += 1
            end = min(start + self.batch_size - 1, release.page_count)

            try:
                batch = ReleaseBatch(
                    batch_num=(start - 1) // self.batch_size + 1,
                    release=release,
                    start_page_num=start,
                    end_page_num=end,
                )
                batches.append(batch)

            except Exception as e:
                logger.error(
                    f"Failed to create batch for {release.filename} "
                    f"pages {start}-{end}: {e}",
                    exc_info=True,
                )

        logger.info(
            f"Created {len(batches)}/{total_batches} batches for "
            f"release {release.filename}: "
            f"{self.batch_size} batch size"
        )
        return batches
