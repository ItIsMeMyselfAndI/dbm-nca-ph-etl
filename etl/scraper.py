from etl.pdf.loader import load_nca_to_db
from etl.pdf.transformer import load_sample_bytes, parse_nca_bytes


def main():
    # releases = get_nca_pdf_releases()
    bytes = load_sample_bytes()
    sample_release = {"title": "SAMPLE NCA", "year": "2025",
                      "filename": "sample_nca.pdf", "url": "#"}
    records = parse_nca_bytes(10, bytes, sample_release)
    load_nca_to_db(sample_release, records)


if __name__ == "__main__":
    main()
