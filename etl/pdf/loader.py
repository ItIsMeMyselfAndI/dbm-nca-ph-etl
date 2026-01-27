from typing import Dict, List
from client import supabase


def _insert_release(release: Dict):
    result = supabase.table("release").insert(release).execute()
    data = result.model_dump(mode="python").get("data")
    if data:
        db_release = data[0]
        print(f"[INFO] Inserted 'NCA-{release["year"]}' release to db")
    else:
        db_release = None
        print(
            f"[ERROR] Failed inserting 'NCA-{release["year"]}' release to db")
    # print(db_release)
    return db_release


def _add_release_id_to_records(release_id: str, records: List[Dict]):
    for record in records:
        record["release_id"] = release_id


def _insert_records(release: Dict, records: List[Dict]):
    result = supabase.table("nca").insert(records).execute()
    db_records = result.model_dump(mode="python").get("data")
    if db_records and len(db_records) == len(records):
        print(f"[INFO] Inserted 'NCA-{release["year"]}' records to db")
    else:
        db_records = None
        print(
            f"[ERROR] Failed inserting 'NCA-{release["year"]}' records to db")
    # print(db_records)
    return db_records


def load_nca_to_db(release: Dict, records: List[Dict]):
    db_release = _insert_release(release)
    if db_release:
        _add_release_id_to_records(db_release["id"], records)
        # print(records)
        db_records = _insert_records(release, records)
