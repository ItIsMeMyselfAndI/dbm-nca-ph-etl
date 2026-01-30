from datetime import datetime, timedelta
import time
from typing import Dict, Literal
from etl.client import supabase
from etl.pdf.extractor import (
    download_nca_pdf_bytes, get_nca_pdf_releases
)
from etl.pdf.loader import delete_latest_nca_in_db, load_nca_to_db
from etl.pdf.transformer import parse_nca_bytes_2_db_data
from etl.utils.parse_nca_pdf_2_bytes import parse_nca_pdf_2_bytes
from etl.utils.nca_bytes_2_pdf import nca_bytes_2_pdf


def get_db_last_release():
    result = supabase.table("release").select(
        "*").order("id", desc=True).limit(1).execute()
    data = result.model_dump(mode="python").get("data")
    if data:
        db_row = data[0]
    else:
        db_row = None
    return db_row


def get_db_last_record():
    result = supabase.table("record").select(
        "*").order("id", desc=True).limit(1).execute()
    data = result.model_dump(mode="python").get("data")
    if data:
        db_row = data[0]
    else:
        db_row = None
    return db_row


def process_data(page_count: Literal["all"] | int,
                 release: Dict):
    print("")
    bytes = download_nca_pdf_bytes(release)
    nca_bytes_2_pdf(release, bytes)
    data = parse_nca_bytes_2_db_data(page_count, bytes, release)
    load_nca_to_db(release, data["records"], data["allocations"])


def main(table_count: Literal["all"] | int):
    """
        - if the database has a last release and its year matches
                the current year, it updates that release.
        - if the last release is from a previous year, it adds
                all newer releases.
        - if the database is empty, it initializes it with all
                available releases.
    """
    print("\nUpdating NCA db...")
    print("\n--------------------------------\n")
    last_release = get_db_last_release()
    releases = get_nca_pdf_releases()
    if len(releases) == 0:
        print("[ERROR] Failed to update NCA db")
        return
    releases.sort(key=lambda x: x["year"])
    curr_year = datetime.now().year
    # curr_year = 2027
    if last_release:
        # replace last release
        if last_release["year"] == curr_year:
            latest_release = releases[-1]
            delete_latest_nca_in_db(last_release)
            process_data(table_count, latest_release)
        # add all new/latest release
        else:
            has_added = False
            for release in releases:
                if release["year"] > last_release["year"]:
                    process_data(table_count, release)
                    has_added = True
            if not has_added:
                print("[*]\tNo new release found")
    else:
        # add all releases
        for release in releases:
            if release["year"] >= 2024:
                process_data(table_count, release)
    print("\n--------------------------------\n")
    print("Updated NCA db successfully\n")


def test(table_count: Literal["all"] | int):
    # releases = get_nca_pdf_releases()
    bytes = parse_nca_pdf_2_bytes("./releases/NCA_2024.pdf")
    sample_release = {"title": "SAMPLE NCA", "year": "2025",
                      "filename": "sample_nca.pdf", "url": "#"}
    data = parse_nca_bytes_2_db_data(table_count, bytes, sample_release)
    load_nca_to_db(sample_release, data["records"], data["allocations"])


if __name__ == "__main__":
    prev_time = time.time()
    # test(10)
    # main(10)
    main("all")
    curr_time = time.time()
    elapsed = str(timedelta(seconds=curr_time - prev_time)).split(":")
    print(f"Elapsed time: {elapsed[0]}h {elapsed[1]}m {elapsed[2]}s")
