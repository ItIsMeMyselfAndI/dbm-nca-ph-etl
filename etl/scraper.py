from etl.pdf.extractor import download_nca_pdf_bytes, get_nca_pdf_releases
from etl.pdf.loader import load_nca_to_db
from etl.pdf.transformer import load_sample_bytes, parse_nca_bytes


def main():
    print("[INFO] Updating NCA db...")
    releases = get_nca_pdf_releases()
    for release in releases:
        if release["year"] >= 2024:
            bytes = download_nca_pdf_bytes(release)
            records = parse_nca_bytes("all", bytes, release)
            load_nca_to_db(release, records)
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
