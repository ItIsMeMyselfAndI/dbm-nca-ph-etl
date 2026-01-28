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
    website_name = "PH-DBM"
    # scrape
    print(f"[INFO] Scraping '{website_name}' website for downloadable pdfs...")
    res = requests.get(NCA_PAGE, timeout=30)
    try:
        res.raise_for_status()
    except Exception as e:
        print(f"[!] Failed scraping '{website_name}'")
        print(f"\t{e}")
        print("[*] Retrying...")
        get_nca_pdf_releases()
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
            "year": int(str(year))
        })
    # logs
    length = len(pdf_releases)
    print(f"[*]\tFound {length} {"pdfs" if length > 1 else "pdf"}")
    for i, link in enumerate(pdf_releases):
        print(f"\t({i})\tTitle: {link["title"]}")
        print(f"\t\tFilename: {link["filename"]}")
        print(f"\t\tYear: {link["year"]}")
        print(f"\t\tURL: {link["url"]}")
    print("[INFO] Finished scraping PH-DBM website")
    return pdf_releases


def download_nca_pdf_bytes(release: Dict) -> BytesIO:
    url = release["url"]
    filename = release["filename"]
    print(f"[INFO] Downloading '{filename}' into bytes...")
    res = requests.get(url)
    try:
        res.raise_for_status()
    except Exception as e:
        print(f"[!] Failed downlading '{filename}'")
        print(f"\t{e}")
        print("[*] Retrying...")
        download_nca_pdf_bytes(release)
    print(f"[INFO] Finished downloading '{filename}'")
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
        # save_nca_pdf(release, bytes)
