import os
from io import BytesIO
from typing import Dict


def nca_bytes_2_pdf(release: Dict, bytes: BytesIO):
    folder = "releases"
    filename = release["filename"]
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, filename), "wb") as f:
        f.write(bytes.getvalue())
        print(f"[INFO] Saved '{filename}' to {folder}/")
