from typing import List
import pandas as pd
import numpy as np

from src.core.entities.allocation import Allocation
from src.core.entities.nca_data import NCAData
from src.core.entities.record import Record
from src.core.interfaces.data_cleaner import DataCleanerProvider


class PdDataCleaner(DataCleanerProvider):
    def __init__(
        self,
        allocation_comumns: List[str],
        record_columns: List[str],
        valid_columns: List[str],
    ):
        self.allocation_columns = allocation_comumns
        self.record_columns = record_columns
        self.valid_columns = valid_columns

    def clean_raw_data(
        self,
        raw_rows: List[List[str | None]],
        release_id: str,
    ) -> NCAData:
        df = self._convert_raw_to_df(raw_rows)
        df = self._insert_nca_group_spacers(df)
        df = self._remove_header_rows(df)

        df["nca_number"] = df["nca_number"].replace("", np.nan)
        df["nca_number"] = df["nca_number"].ffill()
        df = pd.DataFrame(
            df.groupby("nca_number", as_index=False).agg(
                {
                    "nca_type": lambda col: self._join_col_to_str(col),
                    "released_date": lambda col: self._join_col_to_str(col),
                    "department": lambda col: self._join_col_to_str(col),
                    "agency": lambda col: col,
                    "operating_unit": lambda col: col,
                    "amount": lambda col: col,
                    "purpose": lambda col: self._join_col_to_str(col),
                }
            )
        )
        df = df.dropna(how="all").dropna(subset="nca_number")
        df = df.replace(np.nan, "")
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        df["release_id"] = release_id

        df_records = self._create_df_records(df)

        if df_records.shape[0] < 1:
            return NCAData(records=[], allocations=[])
        df_allocations = self._create_df_allocations(df)

        records = self._convert_df_to_object_list(df_records, Record)
        allocations = self._convert_df_to_object_list(df_allocations, Allocation)
        data = NCAData(records=records, allocations=allocations)
        return data

    def _convert_raw_to_df(self, raw_rows: List[List[str | None]]):
        table_header = [
            item.lower().replace(" ", "_") if item else "" for item in raw_rows[0]
        ]
        df = pd.DataFrame(raw_rows[1:], columns=table_header)
        df = pd.DataFrame(df[self.valid_columns])
        return df

    def _insert_nca_group_spacers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Only add a spacer if:
        1. prev nca num is not empty
        2. nca num changed
        3. both current and last are not empty
        """
        new_rows = []
        last_nca = None

        for _, row in df.iterrows():
            current_nca = row["nca_number"]

            # Use pd.isna() for the most reliable null-checking in DataFrames
            curr_is_valid = pd.notna(current_nca) and str(current_nca).strip() != ""
            last_is_valid = pd.notna(last_nca) and str(last_nca).strip() != ""

            if (
                last_is_valid and curr_is_valid and current_nca != last_nca
            ):  # pyright: ignore
                # Adding the spacer
                empty_row = pd.Series([""] * len(df.columns), index=df.columns)
                new_rows.append(empty_row)

            new_rows.append(row)
            last_nca = current_nca

        return pd.DataFrame(new_rows).reset_index(drop=True)

    def _remove_header_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        1. lower case chars
        2. replace multiple spaces with single space
        3. replace spaces with underscores
        4. check if all columns in row are the same as column names
        """

        def is_header_row(row):
            processed_row = (
                row.astype(str)
                .str.lower()
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
                .str.replace(" ", "_")
            )
            return all(processed_row == df.columns)

        df_result = df.apply(is_header_row, axis=1)
        return pd.DataFrame(df[~df_result])

    def _join_col_to_str(self, col: List[str]):
        joined_str = ""
        last_item = None
        for item in col:
            if last_item == item:
                continue
            if item and item is not np.nan:
                joined_str += " " + item
                last_item = item
        return joined_str

    def _create_df_records(self, df: pd.DataFrame):
        df["released_date"] = pd.to_datetime(
            df["released_date"], errors="coerce"
        ).dt.strftime("%Y-%m-%dT%H:%M:%S")
        df_records = pd.DataFrame(df[self.record_columns]).drop_duplicates(
            subset="nca_number"
        )
        return df_records

    def _create_df_allocations(self, df: pd.DataFrame):
        """
        for row in df:
            if row all Nan: push row to allocations
            else: concatenate row items to allocations[-1] items
        """
        df = self._insert_nca_group_spacers(df)
        new_df = pd.DataFrame(df[self.allocation_columns])
        new_df = new_df.explode(self.allocation_columns[1:], ignore_index=True)
        df_allocations = [new_df.iloc[0]]
        for _, row in new_df.iloc[1:].iterrows():
            if (row[self.allocation_columns[1:]] == "").all():
                df_allocations.append(row)
            else:
                last_idx = len(df_allocations) - 1
                df_allocations[last_idx]["nca_number"] = row["nca_number"]
                df_allocations[last_idx]["agency"] += " " + row["agency"]
                df_allocations[last_idx]["operating_unit"] += (
                    " " + row["operating_unit"]
                )
                df_allocations[last_idx]["amount"] += " " + row["amount"]
        df_allocations = pd.DataFrame(df_allocations).replace("", np.nan)
        df_allocations = pd.DataFrame(
            df_allocations.dropna(subset=self.allocation_columns[1:], how="all")
        )
        df_allocations = df_allocations.fillna("").map(lambda x: x.strip())
        df_allocations["amount"] = pd.to_numeric(
            df_allocations["amount"].str.replace(",", ""), errors="coerce"
        )
        df_allocations = df_allocations.dropna(subset=["amount"])
        return df_allocations

    def _convert_df_to_object_list(
        self, df: pd.DataFrame, object_class: type[Record] | type[Allocation]
    ) -> List:
        obj_list = []
        dict_list = df.to_dict(orient="records")
        for row in dict_list:
            obj = object_class(**row)
            obj_list.append(obj)
        return obj_list
