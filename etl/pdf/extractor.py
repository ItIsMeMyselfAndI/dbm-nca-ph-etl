import os
from typing import Dict, List
import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
from datetime import datetime

BASE_URL = "https://www.dbm.gov.ph"
NCA_PAGE = "https://www.dbm.gov.ph/index.php/notice-of-cash-allocation-nca-listing"


def get_nca_pdf_releases() -> List[Dict]:
    # scrape
    res = requests.get(NCA_PAGE, timeout=30)
    res.raise_for_status()
    soup = BeautifulSoup(res.content, "html.parser")
    pdf_releases = []
    for elem in soup.find_all("a", href=re.compile(r".*NCA.*\.pdf$", re.I)):
        url = str(elem.get("href", ""))
        if not url:
            continue
        if url.startswith("/"):
            url = BASE_URL + url
        title = elem.get_text(strip=True)
        filename = url.split("/")[-1]
        year = None
        if "UPDATED" in filename:
            year = datetime.now().year
        else:
            match = re.search(r'(\d{4})', filename)
            if match:
                year = match.group(1)
        pdf_releases.append({
            "title": title,
            "url": url,
            "filename": filename,
            "year": year
        })
    # logs
    length = len(pdf_releases)
    print(f"[INFO] {length if length > 0 else "No"} {
          "pdfs" if length > 1 else "pdf"} found")
    for i, link in enumerate(pdf_releases):
        print(f"({i})\tTitle: {link["title"]}")
        print(f"\tFilename: {link["filename"]}")
        print(f"\tYear: {link["year"]}")
        print(f"\tURL: {link["url"]}")
    return pdf_releases


def download_nca_pdf_bytes(release: Dict) -> BytesIO:
    url = release["url"]
    filename = release["filename"]
    res = requests.get(url)
    res.raise_for_status()
    print(f"[INFO] Downloaded '{filename}'")
    return BytesIO(res.content)


# test
def save_nca_pdf(release: Dict, bytes: BytesIO):
    folder = "releases"
    filename = release["filename"]
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, filename), "wb") as f:
        f.write(bytes.getvalue())
        print(f"[INFO] Saved '{filename}' to {folder}/")


if __name__ == "__main__":
    pdf_releases = get_nca_pdf_releases()
    for release in pdf_releases:
        bytes = download_nca_pdf_bytes(release)
        save_nca_pdf(release, bytes)
