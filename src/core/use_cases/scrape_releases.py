from copy import Error
import logging
from typing import List
from core.interfaces.scraper import ScraperProvider
from core.interfaces.storage import StorageProvider
from src.core.entities.release import Release
from src.core.interfaces.parser import ParserProvider
from src.core.interfaces.repository import RepositoryProvider

logger = logging.getLogger(__name__)


class ScrapeReleases:
    def __init__(self,
                 scraper: ScraperProvider,
                 storage: StorageProvider,
                 parser: ParserProvider,
                 repository: RepositoryProvider):
        self.scraper = scraper
        self.storage = storage
        self.parser = parser
        self.repository = repository

    def run(self, oldest_release_year: int = 2024) -> List[Release]:
        logger.info(f"Scraping for releases since {oldest_release_year}...")

        try:
            releases = self.scraper.get_releases(oldest_release_year)
            logger.info(f"Found {len(releases)} releases")
        except Exception as e:
            logger.critical(f"Failed to fetch release list: {e}")
            raise e

        success_count = 0
        for release in releases:
            storage_path = (f"{self.storage.get_base_storage_path()}/"
                            f"{release.filename}")
            try:
                self._save_release(storage_path, release)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to ingest {storage_path}: {e}")

        logger.info(f"Successfully synced {
                    success_count}/{len(releases)} files.")

        logger.info("Filtering new/modified releases...")
        filtered_releases = self._filter_new_or_updated_releases(releases)
        logger.info(
            f"Filtered only new/updated releases: "
            f"{len(filtered_releases)}/{len(releases)} remained")

        # <test ----------->
        # return releases
        # </test ----------->
        return filtered_releases

    def _save_release(self, storage_path: str, release: Release):
        data = self.scraper.download_release(release)
        if data.getbuffer().nbytes == 0:
            raise Error("Downloaded file is empty.")

        self.storage.save_file(storage_path, data)
        logger.info(f"Synced: {storage_path}")

    def _filter_new_or_updated_releases(self, releases: List[Release]):
        """
            if db release empty
                include all releases
            else
                for each release
                    if stored release file metadata != db release metadata
                        include release
        """
        filtered_releases = []
        for release in releases:
            db_release = self.repository.get_release(release.id)
            storage_path = (f"{self.storage.get_base_storage_path()}/"
                            f"{release.filename}")
            file_release_metadata = self.parser.get_metadata(storage_path)

            release.file_meta_created_at = file_release_metadata.created_at
            release.file_meta_modified_at = file_release_metadata.modified_at

            if not db_release:
                filtered_releases.append(release)
                continue
            if not file_release_metadata:
                continue

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
            else:
                logger.debug(f"No changes for {release.filename}. Skipping.")

        return filtered_releases
