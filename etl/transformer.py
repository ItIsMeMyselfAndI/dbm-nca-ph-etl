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


def _join_col_to_str(col: List[str]):
    filtered = map(lambda x: x if type(x) is str else '', col)
    return ' '.join(filter(None, filtered)).strip()


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


def _join_col_to_list(col: List[str],
                      item_type: Literal["str", "int", "float"]):
    col = list(col)
    type_casters = {"str": str, "int": int, "float": float}
    type_caster = type_casters[item_type]
    values = [col[0]]
    for entry in col[1:]:
        if "\n" not in entry:
            value = entry.replace("\n", "")
            if value:
                values.append(value)
                # print(value)
        else:
            values[-1] += entry
    for i, entry in enumerate(values):
        if item_type == "int" or item_type == "float":
            value = entry.replace(",", "")
        else:
            value = entry.replace("", "None")
        values[i] = type_caster(value)
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


def parse_nca_bytes(bytes: BytesIO, release: Dict | None = None):
    with pdfplumber.open(bytes) as pdf:
        records = []
        for page_num, page in enumerate(pdf.pages):
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
                "operating_unit": lambda col: _join_col_to_list(col, "str"),
                "amount": lambda col: _join_col_to_list(col, "float"),
                "purpose": lambda col: _join_col_to_str(col),
            })
            df = pd.DataFrame(df_merged)
            new_records = df.to_dict(orient="records")
            records.append(new_records)
            print(f"[INFO] Parsed {df.shape[0]} rows successfully")
            buff = io.StringIO()
            df.info(buf=buff)
            print(_indent_str_buff(buff))
            # _save_nca_xls(release, df, page_num)


bytes = load_sample_pdf_bytes()
parse_nca_bytes(bytes)
