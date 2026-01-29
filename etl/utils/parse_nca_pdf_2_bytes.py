from io import BytesIO


def parse_nca_pdf_2_bytes(filename: str):
    with open(filename, "rb") as pdf:
        bytes = pdf.read()
    return BytesIO(bytes)
