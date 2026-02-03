# scraper
BASE_URL = "https://www.dbm.gov.ph"
NCA_PAGE = "https://www.dbm.gov.ph/index.php/notice-of-cash-allocation-nca-listing"
WEBSITE_NAME = "PH-DBM"

# local storage
BASE_STORAGE_PATH = "releases/"

BATCH_PAGE_COUNT = 5

# table
VERT_LINES = [
    19.439992224, 133.439946624,
    182.159927136, 275.9998896,
    389.15984433600005, 500.159799936,
    638.159744736, 737.9997048, 1100.00000
]
TABLE_COLUMNS = [
    "nca_number", "nca_type", "approved_date", "released_date", "department",
    "agency", "operating_unit", "amount", "purpose", "remarks"
]
VALID_COLUMNS = [
    "nca_number", "nca_type", "released_date", "department",
    "agency", "operating_unit", "amount", "purpose",
]
RECORD_COLUMNS = [
    "nca_number", "nca_type", "released_date",
    "department", "purpose", "release_id"
]
ALLOCATION_COLUMNS = [
    "nca_number", "agency", "operating_unit", "amount"
]

# database
DB_BULK_SIZE = 500
