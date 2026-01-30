from io import StringIO, BytesIO
from typing import Dict, List, Literal
import pdfplumber
from pdfplumber.page import Page
import pandas as pd
import numpy as np

from etl.utils.indent_buff_str import indent_buff_str
from etl.utils.nca_bytes_2_pdf import nca_bytes_2_pdf
from etl.utils.parse_nca_pdf_2_bytes import parse_nca_pdf_2_bytes

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
                continue
            x_positions.extend([item[0], item[2]])
    x_positions = list(set(x_positions))
    # print(x_positions)
    return x_positions


def _extract_pdf_table(page: Page, x_positions: List, table_num: int):
    print(f"[*]\tExtracting pdf 'table-{table_num}'...")
    TABLE_SETTINGS = {
        "vertical_strategy": "explicit",
        "horizontal_strategy": "text",
        "explicit_vertical_lines": x_positions,
        "intersection_tolerance": 50,
        "snap_y_tolerance": 3,
        # "join_y_tolerance": 1,
    }
    # ---- test
    # im = page.to_image()
    # im.debug_tablefinder(TABLE_SETTINGS).show()
    # break
    # ----
    table = page.extract_table(TABLE_SETTINGS)
    print("[*]\tExtracted pdf table successfully")
    return table


def _convert_table_to_df(table: List[List[str | None]]):
    print("[*]\tConverting table into DataFrame...")
    table_header = [item.lower().replace(
        " ", "_") if item else "" for item in table[0]]
    try:
        df = pd.DataFrame(table[1:], columns=table_header)
        df = pd.DataFrame(df[VALID_HEADERS])
        # print(df.columns.values)
        print("[*]\tConverted table successfully")
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


def _clean_df(df: pd.DataFrame, table_num: int):
    print("[*]\tCleaning DataFrame...")
    df["nca_number"] = df["nca_number"].replace('', np.nan)
    df["nca_number"] = df["nca_number"].ffill()
    # print(df[["nca_number", "nca_type", "released_date",
    #       "department", "agency", "amount"]].head(20))
    df_merged = df.groupby("nca_number", as_index=False).agg({
        "nca_type": lambda col: _join_col_to_str(col),
        "released_date": lambda col: _join_col_to_str(col),
        "department": lambda col: _join_col_to_str(col),
        "agency": lambda col: col,
        "operating_unit": lambda col: col,
        "amount": lambda col: col,
        "purpose": lambda col: _join_col_to_str(col),
    })
    df = pd.DataFrame(df_merged)
    df["table_num"] = table_num
    df = df.dropna(how="all").dropna(subset="nca_number")
    # break
    df = df.replace(np.nan, "")
    print("[*]\tCleaned DataFrame successfully")
    return df


def _get_records_df(df: pd.DataFrame, table_num: int):
    # print("[*]\tFormatting records dataframe...")
    df["table_num"] = table_num
    df_records = pd.DataFrame(
        df[["id", "nca_number", "nca_type", "released_date",
            "department", "purpose", "table_num"]]
    ).drop_duplicates(subset="nca_number")
    # print("[*]\tFormatted records dataframe successfully")
    return df_records


def _get_allocations_df(df: pd.DataFrame):
    """
        for row in df:
            if row all Nan: push row to allocations
            else: concatenate row items to allocations[-1] items
    """
    # print("[*]\tFormatting allocations dataframe...")
    header = ["agency",
              "operating_unit", "amount"]
    new_df = pd.DataFrame(df[header])
    new_df["record_id"] = df["id"]
    new_df = new_df.explode(header, ignore_index=True)
    new_df = new_df.fillna("")
    # print("new_df")
    # print(new_df.tail(50))
    df_allocations = [new_df.iloc[0]]
    for _, row in new_df.iloc[1:].iterrows():
        if (row[header] == "").all():
            df_allocations.append(row)
        else:
            last_idx = len(df_allocations) - 1
            df_allocations[last_idx]["agency"] += " " + row["agency"]
            df_allocations[last_idx]["operating_unit"] += " " + \
                row["operating_unit"]
            df_allocations[last_idx]["amount"] += " " + row["amount"]
    df_allocations = pd.DataFrame(df_allocations).replace("", np.nan)
    df_allocations = pd.DataFrame(
        df_allocations.dropna(subset=header, how="all"))
    df_allocations[header] = df_allocations[header].fillna(
        "").map(lambda x: x.strip())
    df_allocations["amount"] = pd.to_numeric(
        df_allocations["amount"].str.replace(",", ""), errors="coerce")
    df_allocations = df_allocations.dropna(subset=["amount"])
    # print("alloc")
    # print(allocations.tail(50))
    # print("[*]\tFormatted allocations dataframe successfully")
    return df_allocations


def parse_nca_bytes_2_db_data(page_count: Literal["all"] | int,
                              bytes: BytesIO, release: Dict,
                              last_record: Dict | None):
    print(f"[INFO] Preparing 'NCA-{release["year"]}' data...")
    records: List[Dict] = []
    allocations: List[Dict] = []
    x_positions = X_POSITIONS
    last_record_id = last_record["id"] + 1 if last_record else 0
    with pdfplumber.open(bytes) as pdf:
        for page_num, page in enumerate(pdf.pages):
            if page_num == 0:
                x_positions = _get_x_positions_using_text(page)
                if len(x_positions) <= 1:
                    x_positions = _get_x_positions_using_table(page)
            print(f"[*]\t> Parsing NCA-{release["year"]
                                        } (table-{page_num}) data...")
            # ---- test
            # if page_num != 48:
            #     continue
            # ----
            if page_count != "all" and page_num > page_count:
                break
            table = _extract_pdf_table(page, x_positions, page_num)
            if not table:
                continue
            df = _convert_table_to_df(table)
            if df is None:
                continue
            df = _clean_df(df, page_num)
            df["id"] = range(last_record_id,
                             last_record_id + df.shape[0])
            last_record_id += df.shape[0]
            df_records = _get_records_df(df, page_num)
            if df_records.shape[0] < 1:
                print("[*]\tExtracted 0 rows")
                print(f"[*]\t> Parsed NCA-{release["year"]
                                           } (table-{page_num}) successfully")
                continue
            df_allocations = _get_allocations_df(df)
            # print(df_records["nca_number"])
            # print(df_allocations["nca_number"])
            # print(df.values)
            # break
            new_records = df_records.to_dict(orient="records")
            new_allocations = df_allocations.to_dict(orient="records")
            records.extend(new_records)
            allocations.extend(new_allocations)
            buff = StringIO()
            df.info(buf=buff)
            row_count = df.shape[0]
            print(
                f"[*]\tExtracted {row_count} {"rows" if row_count > 1 else "row"}")
            # print(indent_buff_str(buff, 2))
            # nca_df_2_xlsx(release, df, page_num)
            print(f"[*]\t> Parsed NCA-{release["year"]
                                       } (table-{page_num}) successfully")
    print(f"[INFO] Prepared 'NCA-{release["year"]}' data successfully")
    data = {"records": records, "allocations": allocations}
    return data


if __name__ == "__main__":
    bytes = parse_nca_pdf_2_bytes("./releases/NCA-2016.pdf")
    sample_release = {"title": "SAMPLE NCA", "year": "2025",
                      "filename": "sample_nca.pdf", "url": "#"}
    data = parse_nca_bytes_2_db_data(50, bytes, sample_release, None)
    # print(data["records"])
    # print(data["allocations"])
