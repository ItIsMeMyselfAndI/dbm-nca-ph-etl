from datetime import datetime
from typing import Dict, Literal
from etl.client import supabase
from etl.pdf.extractor import (
    download_nca_pdf_bytes, get_nca_pdf_releases
)
from etl.pdf.loader import delete_latest_nca_in_db, load_nca_to_db
from etl.pdf.transformer import parse_nca_bytes_2_records
from etl.utils.parse_nca_pdf_2_bytes import parse_nca_pdf_2_bytes
from etl.utils.nca_bytes_2_pdf import nca_bytes_2_pdf


def get_last_db_release():
    result = supabase.table("release").select(
        "*").order("id", desc=True).limit(1).execute()
    data = result.model_dump(mode="python").get("data")
    if data:
        db_release = data[0]
    else:
        db_release = None
    return db_release


def process_records(page_count: Literal["all"] | int, release: Dict):
    bytes = download_nca_pdf_bytes(release)
    nca_bytes_2_pdf(release, bytes)
    records = parse_nca_bytes_2_records(page_count, bytes, release)
    load_nca_to_db(release, records)


def main():
    """
        - if the database has a last release and its year matches
                the current year, it updates that release.
        - if the last release is from a previous year, it adds
                all newer releases.
        - if the database is empty, it initializes it with all
                available releases.
    """
    print("[INFO] Updating NCA db...")
    db_last_release = get_last_db_release()
    releases = get_nca_pdf_releases()
    releases.sort(key=lambda x: x["year"])
    curr_year = datetime.now().year
    # curr_year = 2027
    if db_last_release:
        # replace last release
        if db_last_release["year"] == curr_year:
            latest_release = releases[-1]
            delete_latest_nca_in_db(db_last_release)
            process_records("all", latest_release)
            print("[*]\tUpdated last release")
        # add all new/latest release
        else:
            has_added = False
            for release in releases:
                if release["year"] > db_last_release["year"]:
                    process_records("all", release)
                    print("[*]\tAdded new release")
                    has_added = True
            if not has_added:
                print("[*]\tNo new release found")
    else:
        # add all releases
        for release in releases:
            process_records("all", release)
            print("[*]\tAdded new release")
    print("[INFO] Updated NCA db successfully")


def test():
    # releases = get_nca_pdf_releases()
    bytes = parse_nca_pdf_2_bytes("./sample_nca.pdf")
    sample_release = {"title": "SAMPLE NCA", "year": "2025",
                      "filename": "sample_nca.pdf", "url": "#"}
    records = parse_nca_bytes_2_records(10, bytes, sample_release)
    load_nca_to_db(sample_release, records)


if __name__ == "__main__":
    # test()
    main()
