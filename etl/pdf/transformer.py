from io import BytesIO
import io
import os
from typing import Dict, List, Literal
import pdfplumber
from pdfplumber.page import Page
import pandas as pd
import numpy as np

X_POSITIONS = [
    19.439992224, 133.439946624,
    182.159927136, 275.9998896,
    389.15984433600005, 500.159799936,
    638.159744736, 737.9997048, 1000.00000
]
POSSIBLE_HEADERS = [
    "nca_number", "nca_type", "approved_date", "released_date", "department",
    "agency", "operating_unit", "amount", "purpose", "remarks"
]
VALID_HEADERS = [
    "nca_number", "nca_type", "released_date", "department",
    "agency", "operating_unit", "amount", "purpose",
]


def load_sample_bytes(filename: str):
    with open(filename, "rb") as pdf:
        bytes = pdf.read()
    return BytesIO(bytes)


def _get_x_positions_using_text(page: Page):
    target_phrases = POSSIBLE_HEADERS
    found_phrases = set()
    x_positions = []
    words = page.extract_words()
    texts = [w["text"] for w in words]
    for phrase in target_phrases:
        # print(texts[:10])
        phrase_words = phrase.lower().split("_")
        n = len(phrase_words)
        for i in range(len(texts) - n + 1):
            curr_phrase = "_".join(texts[i:i+n]).lower()
            if curr_phrase == phrase:
                x_positions.append(words[i]["x0"])
                found_phrases.add(phrase)
                break
    if "remarks" not in found_phrases:
        x_positions.append(1000)
    # TABLE_SETTINGS = {
    #     "vertical_strategy": "explicit",
    #     "explicit_vertical_lines": x_positions,
    #     "horizontal_strategy": "text",
    #     "intersection_tolerance": 100,
    #     "snap_y_tolerance": 3,
    #     # "join_y_tolerance": 1,
    # }
    # im = page.to_image()
    # im.debug_tablefinder(TABLE_SETTINGS).show()
    # print(found_phrases)
    # print(x_positions)
    return x_positions


def _get_x_positions_using_table(page: Page):
    x_positions = []
    table = page.find_table()
    if table:
        col_positions = table.rows[0].cells
        for item in col_positions:
            if not item:
                return
            x_positions.extend([item[0], item[2]])
    x_positions = list(set(x_positions))
    # print(x_positions)
    return x_positions


def _convert_table_to_df(table: List[List[str | None]]):
    print("[*]\tConverting table into DataFrame...")
    table_header = [item.lower().replace(
        " ", "_") if item else "" for item in table[0]]
    try:
        df = pd.DataFrame(table[1:], columns=table_header)
        df = pd.DataFrame(df[VALID_HEADERS])
        # print(df.columns.values)
        print("[*]\tFinished conversion")
        return df
    except Exception:
        print("[!]\tPDF is unreadable")
        print("\tSkipped table")
        return None


def _join_col_to_str(col: List[str]):
    joined_str = ""
    for item in col:
        if item and item is not np.nan:
            joined_str += " " + item
    return joined_str


def _sep_op_units_to_list(col: List[str]):
    values = ['']
    for val in col:
        if val and val is not np.nan:
            values[-1] = (values[-1] + " " + val).strip()
        elif not val and values[-1]:
            values.append('')
    if not values[-1]:
        values.pop()
    # print(values)
    return values


def _sep_amounts_to_list(col: List[str]):
    """
        step 1: '23434.00233423.652323423.50234234.44'
        step 2: ['23434', '00233423', '652323423', '50234234', '44']
        step 3: [23434.00, 233423.65, 2323423.50, 234234.44]
    """
    # print(col)
    string = _join_col_to_str(col).replace(" ", "").replace(",", "")
    string_lst = string.split(".")
    values = []
    for i, item in enumerate(string_lst[0:-1]):
        val = item + "." + string_lst[i+1][0:2]
        values.append(float(val))
        pass
    # print(values)
    return values


def _double_indent_str_buff(buff: io.StringIO):
    string = buff.getvalue()
    indented_str = '\n'.join('\t\t' + line for line in string.splitlines())
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
                    bytes: BytesIO, release: Dict):
    print(f"[INFO] Preparing 'NCA-{release["year"]}' for db operations...")
    records: List[Dict] = []
    x_positions = X_POSITIONS
    with pdfplumber.open(bytes) as pdf:
        for page_num, page in enumerate(pdf.pages):
            if page_num == 0:
                x_positions = _get_x_positions_using_text(page)
                if len(x_positions) <= 1:
                    x_positions = _get_x_positions_using_table(page)
            if page_count != "all" and page_num > page_count:
                break
            print(f"[*]\tParsing 'table-{page_num}'...")
            TABLE_SETTINGS = {
                "vertical_strategy": "explicit",
                "explicit_vertical_lines": x_positions,
                "horizontal_strategy": "text",
                "intersection_tolerance": 50,
                "snap_y_tolerance": 3,
                # "join_y_tolerance": 1,
            }
            # im = page.to_image()
            # im.debug_tablefinder(TABLE_SETTINGS).show()
            # break
            table = page.extract_table(TABLE_SETTINGS)
            if not table:
                continue
            df = _convert_table_to_df(table)
            if df is None:
                continue
            df = df.replace('', np.nan)
            df["nca_number"] = df["nca_number"].ffill()
            # print(df[["nca_number", "nca_type", "released_date",
            #       "department", "agency", "amount"]].head(20))
            df_merged = df.groupby("nca_number", as_index=False).agg({
                "nca_type": lambda col: _join_col_to_str(col),
                "released_date": lambda col: _join_col_to_str(col),
                "department": lambda col: _join_col_to_str(col),
                "agency": lambda col: _join_col_to_str(col),
                "operating_unit": lambda col: _sep_op_units_to_list(col),
                "amount": lambda col: _sep_amounts_to_list(col),
                "purpose": lambda col: _join_col_to_str(col),
            })
            df = pd.DataFrame(df_merged)
            df = df.dropna(how="all")
            df["nca_number"] = df["nca_number"].replace(np.nan, "")
            df["table_num"] = page_num
            df = df.sort_values(by=["released_date", "nca_number"],
                                ascending=False)
            # print(df[["nca_number", "nca_type", "released_date",
            #       "department", "agency", "amount"]].head(20))
            # print(df.values)
            # break
            new_records = df.to_dict(orient="records")
            records.extend(new_records)
            buff = io.StringIO()
            df.info(buf=buff)
            row_count = df.shape[0]
            print(f"[*]\tParsed {row_count} {
                  "rows" if row_count > 1 else "row"} of 'table-{page_num}'")
            print(_double_indent_str_buff(buff))
            # _save_nca_xls(release, df, page_num)
    print(f"[INFO] Finished preparing 'NCA-{release["year"]}'")
    return records


if __name__ == "__main__":
    bytes = load_sample_bytes("./releases/UPDATED_NCA.PDF")
    sample_release = {"title": "SAMPLE NCA", "year": "2025",
                      "filename": "sample_nca.pdf", "url": "#"}
    records = parse_nca_bytes("all", bytes, sample_release)
