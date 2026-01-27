from io import BytesIO
import io
import os
from typing import Dict, List, Literal
import pdfplumber
from pdfplumber.page import Page
import pandas as pd


def load_sample_pdf_bytes():
    with open("./sample_nca.pdf", "rb") as pdf:
        bytes = pdf.read()
    return BytesIO(bytes)


def _get_vert_lines(page: Page):
    table = page.find_table()
    vert_lines = []
    if table:
        lines = table.rows[0].cells
        if not lines:
            return
        for i, line in enumerate(lines):
            if not line:
                return
            vert_lines.append(line[0])
            if i == len(lines) - 1:
                vert_lines.append(line[2])
    return vert_lines


def _remove_empty_row(df: pd.DataFrame):
    has_nca_number = df["nca_number"] != ''
    has_nca_type = df["nca_type"] != ''
    has_released_date = df["released_date"] != ''
    has_department = df["department"] != ''
    has_agency = df["agency"] != ''
    has_operating_unit = df["operating_unit"] != ''
    has_amount = df["amount"] != ''
    has_purpose = df["purpose"] != ''
    df_filtered = df[
        has_nca_number | has_nca_type
        | has_released_date | has_department
        | has_agency | has_operating_unit
        | has_amount | has_purpose
    ]
    # print(df_filtered.values)
    return pd.DataFrame(df_filtered)


def _join_col_to_str(col: List[str]):
    filtered = map(lambda x: x if type(x) is str else '', col)
    return ' '.join(filter(None, filtered)).strip()


def _sep_op_units_to_list(col: List[str]):
    """
        step 1: 'Cebu Normal UniversityBicol State College of Applied Sciences and Technology'
        step 2: ai
        step 3: 'Cebu Normal University,Bicol State College of Applied Sciences and Technology'
        step 4: ['Cebu Normal University', 'Bicol State College of Applied Sciences and Technology']
    """
    string = ''.join(filter(lambda x: x != '', col))
    print(string)
    return string


def _sep_amounts_to_list(col: List[str]):
    """
        step 1: '23434.00233423.652323423.50234234.44'
        step 2: ['23434', '00233423', '652323423', '50234234', '44']
        step 3: [23434.00, 233423.65, 2323423.50, 234234.44]
    """
    string = ''.join(
        filter(lambda x: x != '', col)
    ).replace(",", "").replace(" ", "")
    string_lst = string.split(".")
    values = []
    for i, item in enumerate(string_lst[0:-1]):
        val = item + "." + string_lst[i+1][0:2]
        values.append(float(val))
        pass
    print(values)
    return values


def _indent_str_buff(buff: io.StringIO):
    string = buff.getvalue()
    indented_str = '\n'.join('\t' + line for line in string.splitlines())
    return indented_str


def _save_nca_xls(release: Dict | None, df: pd.DataFrame, page_num: int):
    if release:
        filename = f"NCA_{release["year"]}_page_{page_num}.xlsx"
    else:
        filename = f"NCA_page_{page_num}.xlsx"
    folder = "releases"
    os.makedirs(folder, exist_ok=True)
    pathname = os.path.join(folder, filename)
    df.to_excel(pathname, sheet_name=filename, engine="xlsxwriter")
    print(f"[INFO] Saved page {page_num} '{filename}' to {folder}/")


def parse_nca_bytes(page_count: Literal["all"] | int,
                    bytes: BytesIO, release: Dict | None = None):
    records: List[Dict] = []
    with pdfplumber.open(bytes) as pdf:
        for page_num, page in enumerate(pdf.pages):
            if page_count != "all" and page_num > page_count:
                break
            print(f"[INFO] Parsing table in page {page_num}...")
            vert_lines = _get_vert_lines(page)
            TABLE_SETTINGS = {
                "vertical_strategy": "explicit",
                "explicit_vertical_lines": vert_lines,
                "horizontal_strategy": "text",
                "intersection_tolerance": 50,
                "snap_y_tolerance": 3,
                # "join_y_tolerance": 1,
            }
            # im = page.to_image()
            # im.debug_tablefinder(TABLE_SETTINGS).show()
            header = [
                "nca_number", "nca_type", "released_date", "department",
                "agency", "operating_unit", "amount", "purpose",
            ]
            table = page.extract_table(TABLE_SETTINGS)
            if not table:
                continue
            df = pd.DataFrame(table[1:], columns=header)
            df = _remove_empty_row(df)
            df["nca_number"] = df["nca_number"].replace('', None)
            df["nca_number"] = df["nca_number"].ffill()
            df_merged = df.groupby("nca_number", as_index=False).agg({
                "nca_type": "first",
                "released_date": "first",
                "department": lambda col: _join_col_to_str(col),
                "agency": lambda col: _join_col_to_str(col),
                "operating_unit": lambda col: _sep_op_units_to_list(col),
                "amount": lambda col: _sep_amounts_to_list(col),
                "purpose": lambda col: _join_col_to_str(col),
            })
            df = pd.DataFrame(df_merged)
            new_records = df.to_dict(orient="records")
            records.extend(new_records)
            print(f"[INFO] Parsed {df.shape[0]} rows successfully")
            buff = io.StringIO()
            df.info(buf=buff)
            print(_indent_str_buff(buff))
            _save_nca_xls(release, df, page_num)
    return records


if __name__ == "__main__":
    bytes = load_sample_pdf_bytes()
    records = parse_nca_bytes(4, bytes)
