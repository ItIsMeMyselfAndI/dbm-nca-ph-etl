# scraper
BASE_URL = "https://www.dbm.gov.ph"
NCA_PAGE = "https://www.dbm.gov.ph/index.php/notice-of-cash-allocation-nca-listing"
WEBSITE_NAME = "PH-DBM"

# local storage
BASE_STORAGE_PATH = ""

BATCH_SIZE = 10

# table
VERT_LINES = [
    19.439992224,
    133.439946624,
    182.159927136,
    275.9998896,
    389.15984433600005,
    500.159799936,
    638.159744736,
    737.9997048,
    1100.00000,
]
TABLE_COLUMNS = [
    "nca_number",
    "nca_type",
    "approved_date",
    "released_date",
    "department",
    "agency",
    "operating_unit",
    "amount",
    "purpose",
    "remarks",
]
VALID_COLUMNS = [
    "nca_number",
    "nca_type",
    "released_date",
    "department",
    "agency",
    "operating_unit",
    "amount",
    "purpose",
]
RECORD_COLUMNS = [
    "nca_number",
    "nca_type",
    "released_date",
    "department",
    "purpose",
    "release_id",
]
ALLOCATION_COLUMNS = ["nca_number", "agency", "operating_unit", "amount"]

# database
DB_BULK_SIZE = 500

# --------------
# AWS
# --------------

# sqs names
DLQ_NAME = "dbm-nca-ph-failed-queues"
RELEASE_QUEUE_NAME = "dbm-nca-ph-release-queue"
RELEASE_BATCH_QUEUE_NAME = "dbm-nca-ph-release-batch-queue"

# lamda
SCRAPER_FUNCTION_NAME = "dbmScraper"
ORCHESTRATOR_FUNCTION_NAME = "dbmOrchestrator"
WORKER_FUNCTION_NAME = "dbmWorker"
TEARDOWN_FUNCTION_NAME = "dbmTeardown"

# sns topic names
# RELEASE_SNS_TOPIC_NAME = f"{RELEASE_QUEUE_NAME}-idle-topic"
RELEASE_BATCH_SNS_TOPIC_NAME = f"{RELEASE_BATCH_QUEUE_NAME}-idle-topic"

# cloudwatch alaram names
# RELEASE_ALARM_NAME = f"{RELEASE_QUEUE_NAME}-idle-alarm"
RELEASE_BATCH_ALARM_NAME = f"{RELEASE_BATCH_QUEUE_NAME}-idle-alarm"
