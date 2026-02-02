
from typing import Dict, List

from supabase import create_client
from src.core.entities.allocation import Allocation
from src.core.entities.record import Record
from src.core.entities.release import Release
from src.core.interfaces.repository import RepositoryProvider
from src.infrastructure.config import settings


class SupabaseRepository(RepositoryProvider):
    def __init__(self, db_bulk_size: int):
        self.client = create_client(
            settings.SUPABASE_URL,
            settings.SUPABASE_ANON_KEY
        )
        self.db_bulk_size = db_bulk_size

    def get_release(self, id: str) -> Release | None:
        response = self.client.table("release").select(
            "*").eq("id", id).limit(1).execute()
        try:
            db_row = Release(**response.data[0])  # pyright: ignore
            return db_row
        except Exception:
            return None

    def get_last_release(self) -> Release | None:
        response = self.client.table("release").select(
            "*").limit(1).order("id", desc=True).execute()
        try:
            db_row = Release(**response.data[0])  # pyright: ignore
            return db_row
        except Exception:
            return None

    def upsert_release(self, release: Release) -> None:
        data = release.model_dump()
        self._bulk_upsert("release", [data], "id")

    def delete_release(self, id: str) -> None:
        self.client.table("release").delete(
        ).eq("id", id).execute()

    def bulk_upsert_records(self, records: List[Record]) -> None:
        self._bulk_check_data(records)
        data = [record.model_dump() for record in records]
        self._bulk_upsert("record", data, "nca_number")
        self.client.table("record").upsert(data,
                                           on_conflict="nca_number").execute()

    def bulk_insert_allocations(self, allocations: List[Allocation]) -> None:
        self._bulk_check_data(allocations)
        data = [allocation.model_dump() for allocation in allocations]
        self._bulk_insert("allocation", data)

    def _bulk_check_data(self, data):
        if len(data) == 0:
            raise ValueError("No data found.")

    def _bulk_upsert(self, table_name: str,
                     data: List[Dict], on_conflict: str):
        total = len(data)
        for i in range(0, total, self.db_bulk_size):
            bulk = data[i: i + self.db_bulk_size]
            self.client.table(table_name).upsert(
                bulk, on_conflict=on_conflict).execute()

    def _bulk_insert(self, table_name: str, data: List[Dict]):
        total = len(data)
        for i in range(0, total, self.db_bulk_size):
            bulk = data[i: i + self.db_bulk_size]
            self.client.table(table_name).insert(bulk).execute()
