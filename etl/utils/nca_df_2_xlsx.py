import os
from typing import Dict
import pandas as pd


def nca_df_2_xlsx(release: Dict | None, df: pd.DataFrame, page_num: int):
    if release:
        filename = f"NCA_{release["year"]}_page_{page_num}.xlsx"
    else:
        filename = f"NCA_page_{page_num}.xlsx"
    folder = "releases"
    os.makedirs(folder, exist_ok=True)
    pathname = os.path.join(folder, filename)
    df.to_excel(pathname, sheet_name=filename, engine="xlsxwriter")
    print(f"[INFO] Saved page {page_num} '{filename}' to {folder}/")
