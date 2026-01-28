from datetime import datetime
from etl.client import supabase
from etl.pdf.extractor import (
    download_nca_pdf_bytes, get_nca_pdf_releases, save_nca_pdf
)
from etl.pdf.loader import delete_latest_nca_in_db, load_nca_to_db
from etl.pdf.transformer import load_sample_bytes, parse_nca_bytes


def get_last_db_release():
    result = supabase.table("release").select(
        "*").order("id", desc=True).limit(1).execute()
    data = result.model_dump(mode="python").get("data")
    if data:
        db_release = data[0]
    else:
        db_release = None
    return db_release


def main():
    print("[INFO] Updating NCA db...")
    db_last_release = get_last_db_release()
    releases = get_nca_pdf_releases()
    releases.sort(key=lambda x: x["year"])
    curr_year = datetime.now().year
    # curr_year = 2027
    if db_last_release:
        if db_last_release["year"] == curr_year:
            # replace last release
            delete_latest_nca_in_db(db_last_release)
            latest_release = releases[-1]
            bytes = download_nca_pdf_bytes(latest_release)
            save_nca_pdf(latest_release, bytes)
            records = parse_nca_bytes("all", bytes, latest_release)
            load_nca_to_db(latest_release, records)
            print("[*]\tUpdated last release")
        else:
            # add latest release
            latest_release = releases[-1]
            if latest_release["year"] > db_last_release["year"]:
                bytes = download_nca_pdf_bytes(latest_release)
                save_nca_pdf(latest_release, bytes)
                records = parse_nca_bytes("all", bytes, latest_release)
                load_nca_to_db(latest_release, records)
                print("[*]\tAdded new release")
            else:
                print("[*]\tNo new release found")
    else:
        # add all releases
        for release in releases:
            bytes = download_nca_pdf_bytes(release)
            save_nca_pdf(release, bytes)
            records = parse_nca_bytes("all", bytes, release)
            load_nca_to_db(release, records)
            print("[*]\tAdded new releases")
    print("[INFO] Updated NCA db successfully")


def test():
    # releases = get_nca_pdf_releases()
    bytes = load_sample_bytes("./sample_nca.pdf")
    sample_release = {"title": "SAMPLE NCA", "year": "2025",
                      "filename": "sample_nca.pdf", "url": "#"}
    records = parse_nca_bytes(10, bytes, sample_release)
    load_nca_to_db(sample_release, records)


if __name__ == "__main__":
    # test()
    main()
