import json
import time
import logging
from datetime import timedelta
from src.core.entities.release_page_queue_body import ReleasePageQueueBody
from src.core.use_cases.load_allocations_and_records_to_db import (
    LoadRecordsAndAllocationsToDB,
)
from src.infrastructure.adapters.s3_storage import S3Storage
from src.logging_config import setup_logging

from src.core.use_cases.extract_page_table import ExtractPageTable
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
extract_job = ExtractPageTable(storage=storage, parser=parser)
clean_job = PdDataCleaner(
    allocation_comumns=ALLOCATION_COLUMNS,
    record_columns=RECORD_COLUMNS,
    valid_columns=VALID_COLUMNS,
)
load_job = LoadRecordsAndAllocationsToDB(
    data_cleaner=data_cleaner,
    repository=repository,
)


def lambda_handler(event, context):
    logger.info("Starting processing and loading process...")
    prev_time = time.time()

    success_count = 0
    total_pages_count = len(event["Records"])

    for record in event["Records"]:
        try:
            payload = record["body"]
            if isinstance(payload, str):
                payload = json.loads(payload)

            release_page_queue_body = ReleasePageQueueBody(**payload)
            release = release_page_queue_body.release
            page_num = release_page_queue_body.page_num

            extracted_table = extract_job.run(release.filename, page_num)

            if not extracted_table:
                logger.warning(
                    f"Skipped: No table for release " f"{release.id} page-{page_num}"
                )
                continue

            cleaned_table = clean_job.clean_raw_data(extracted_table.rows, release.id)
            load_job.run(release, cleaned_table, page_num)

            success_count += 1

        except Exception as e:
            logger.error(f"Failed record {record['messageId']}: {e}", exc_info=True)

    elapsed = str(timedelta(seconds=time.time() - prev_time)).split(":")
    logger.info(
        f"Batch complete. Success: {success_count}/{total_pages_count}. "
        f"Time: {elapsed[0]}h {elapsed[1]}m {elapsed[2]}s"
    )

    return {
        "statusCode": 200,
        "body": {
            "processed_pages_count": success_count,
            "total_pages_count": total_pages_count,
        },
    }
