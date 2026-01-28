from etl.pdf.extractor import download_nca_pdf_bytes, get_nca_pdf_releases
from etl.pdf.loader import load_nca_to_db
from etl.pdf.transformer import load_sample_bytes, parse_nca_bytes


def main():
    releases = get_nca_pdf_releases()
    for release in releases:
        bytes = download_nca_pdf_bytes(release)
        records = parse_nca_bytes("all", bytes, release)
        load_nca_to_db(release, records)
        break


def test():
    # releases = get_nca_pdf_releases()
    bytes = load_sample_bytes()
    sample_release = {"title": "SAMPLE NCA", "year": "2025",
                      "filename": "sample_nca.pdf", "url": "#"}
    records = parse_nca_bytes("all", bytes, sample_release)
    load_nca_to_db(sample_release, records)


if __name__ == "__main__":
    # test()
    main()
