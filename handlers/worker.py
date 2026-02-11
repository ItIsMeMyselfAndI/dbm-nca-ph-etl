from io import BytesIO
import json
import time
import logging
from datetime import timedelta
from src.core.entities.release_batch import ReleaseBatch
from src.core.use_cases.file_stream_memo_loader import FileBytesMemoLoader
from src.core.use_cases.nca_db_loader import NCADBLoader
from src.core.use_cases.raw_table_cleaner import RawTableCleaner
from src.core.use_cases.raw_table_extractor import RawTableExtractor
from src.infrastructure.adapters.s3_storage import S3Storage
from src.logging_config import setup_logging

from src.infrastructure.adapters.supabase_repository import SupabaseRepository
from src.infrastructure.adapters.pd_data_cleaner import PdDataCleaner
from src.infrastructure.adapters.pdf_parser import PDFParser
from src.infrastructure.constants import (
    ALLOCATION_COLUMNS,
    BASE_STORAGE_PATH,
    DB_BULK_SIZE,
    RECORD_COLUMNS,
    VALID_COLUMNS,
)

setup_logging()
logger = logging.getLogger(__name__)

# adapters
storage = S3Storage(base_storage_path=BASE_STORAGE_PATH)
parser = PDFParser()
data_cleaner = PdDataCleaner(
    allocation_comumns=ALLOCATION_COLUMNS,
    record_columns=RECORD_COLUMNS,
    valid_columns=VALID_COLUMNS,
)
repository = SupabaseRepository(db_bulk_size=DB_BULK_SIZE)

# use cases
file_bytes_loader_job = FileBytesMemoLoader(storage=storage)
extractor_job = RawTableExtractor(storage=storage, parser=parser)
cleaner_job = RawTableCleaner(data_cleaner=data_cleaner)
loader_job = NCADBLoader(
    data_cleaner=data_cleaner,
    repository=repository,
)


def lambda_handler(event, context):
    start_time = time.monotonic()

    for record in event.get("Records", []):
        try:
            payload = record.get("body")
            if isinstance(payload, str):
                payload = json.loads(payload)

            batch = ReleaseBatch(**payload)

            # extractor
            logger.debug(
                f"Extracting {batch.release.filename} "
                f"batch-{batch.batch_num} tables..."
            )

            # file stream memo loader
            file_bytes = file_bytes_loader_job.run(batch.release.filename)
            if not file_bytes:
                continue

            # extractor
            logger.debug(
                f"Extracting {batch.release.filename} "
                f"batch-{batch.batch_num} tables..."
            )

            extracted_table = []
            for i in range(batch.start_page_num, batch.end_page_num + 1):
                table = extractor_job.run(BytesIO(file_bytes), i)
                if not table:
                    logger.warning(
                        f"No tables extracted for {batch.release.filename} "
                        f"batch-{batch.batch_num} page-{i}"
                    )
                    continue
                extracted_table.extend(table)
            if not extracted_table:
                logger.warning(
                    f"No tables extracted for {batch.release.filename} "
                    f"batch-{batch.batch_num}"
                )
                continue
            logger.debug(
                f"Extracted {len(extracted_table)} rows for "
                f"{batch.release.filename} batch-{batch.batch_num}"
            )
            # cleaner
            logger.debug(
                f"Cleaning {batch.release.id} batch-{batch.batch_num} tables..."
            )
            nca_data = cleaner_job.run(extracted_table, batch.release.id)
            logger.debug(
                f"Cleaned data for {batch.release.filename} batch-{batch.batch_num}: "
                f"{len(nca_data.allocations)} allocations, "
                f"{len(nca_data.records)} records"
            )
            # loader
            logger.debug(
                f"Loading {batch.release.id} batch-{batch.batch_num} data to db..."
            )
            loader_job.run(batch.release, nca_data, batch.batch_num)
            logger.debug(
                f"Loaded {batch.release.filename} batch-{batch.batch_num} data to db"
            )

        except Exception as e:
            logger.error(
                f"Failed to process/load release batch: {e}",
                exc_info=True,
            )

    end_time = time.monotonic()
    elapsed_time = timedelta(seconds=end_time - start_time)
    logger.info(f"Total elapsed time: {elapsed_time}")
