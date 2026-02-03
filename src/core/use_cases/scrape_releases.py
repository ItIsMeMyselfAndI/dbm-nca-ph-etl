from copy import Error
from io import BytesIO
import logging
from typing import List, Tuple

from src.core.interfaces.queue import QueueProvider
from src.core.interfaces.scraper import ScraperProvider
from src.core.interfaces.storage import StorageProvider
from src.core.entities.release import Release
from src.core.interfaces.parser import ParserProvider
from src.core.interfaces.repository import RepositoryProvider

logger = logging.getLogger(__name__)


class ScrapeReleases:
    def __init__(self,
                 scraper: ScraperProvider,
                 storage: StorageProvider,
                 parser: ParserProvider,
                 queue: QueueProvider,
                 repository: RepositoryProvider):
        self.scraper = scraper
        self.storage = storage
        self.parser = parser
        self.queue = queue
        self.repository = repository

    def run(self, oldest_release_year: int = 2024) -> List[Release]:
        logger.info(f"Scraping for releases since {oldest_release_year}...")

        # scrape
        try:
            releases = self.scraper.get_releases(oldest_release_year)
            logger.info(f"Found {len(releases)} releases")
        except Exception as e:
            logger.critical(f"Failed to scrape release list: {e}")
            raise e

        # filter
        logger.info("Filtering new/modified releases...")
        filtered_releases, filtered_data = \
            self._filter_new_or_updated_releases(releases)
        logger.info(
            f"Filtered only new/updated releases: "
            f"{len(filtered_releases)}/{len(releases)} remained")

        # save
        success_count = 0
        for i in range(len(filtered_releases)):
            try:
                page_count = self._save_release(filtered_releases[i],
                                                filtered_data[i])
                filtered_releases[i].page_count = page_count

                self.queue.send_data(filtered_releases[i])
                logger.info(f"Queued release file for processing: "
                            f"{filtered_releases[i].filename}")

                success_count += 1

            except Exception as e:
                logger.error(f"Failed to sync "
                             f"{filtered_releases[i].filename}: {e}")

        logger.info(f"Successfully synced {
                    success_count}/{len(releases)} files.")

        # <test ----------->
        # return releases
        # </test ----------->
        return filtered_releases

    def _save_release(self, release: Release, data: BytesIO) -> int:
        if data.getbuffer().nbytes == 0:
            raise Error("Downloaded file is empty.")

        self.storage.save_file(release.filename, data)
        logger.info(f"Synced: {release.filename}")
        page_count = self.parser.get_page_count(data)
        return page_count

    def _filter_new_or_updated_releases(self,
                                        releases: List[Release]
                                        ) -> Tuple[List[Release],
                                                   List[BytesIO]]:
        """
            if db release empty
                include all releases
            else
                for each release
                    if stored release file metadata != db release metadata
                        include release
        """
        filtered_releases = []
        filtered_data = []
        for release in releases:
            db_release = self.repository.get_release(release.id)
            data = self.scraper.download_release(release)
            file_release_metadata = self.parser.get_metadata_by_data(data)

            if not db_release:
                filtered_releases.append(release)
                filtered_data.append(data)
                continue
            if not file_release_metadata:
                self.storage.save_file(release.filename, data)
                filtered_releases.append(release)
                filtered_data.append(data)
                continue

            release.file_meta_created_at = file_release_metadata.created_at
            release.file_meta_modified_at = file_release_metadata.modified_at

            has_changed = (
                db_release.file_meta_created_at !=
                file_release_metadata.created_at
                or
                db_release.file_meta_modified_at !=
                file_release_metadata.modified_at
            )

            if has_changed:
                self.repository.delete_release(release.id)
                logger.info(f"Update detected for {
                            release.filename}. Re-processing...")
                filtered_releases.append(release)
                filtered_data.append(data)
            else:
                logger.debug(f"No changes for {release.filename}. Skipping.")

        return filtered_releases, filtered_data
