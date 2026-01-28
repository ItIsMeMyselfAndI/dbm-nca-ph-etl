import time

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


def _insert_records(release: Dict, records: List[Dict], batch_num: int):
    db_records = []
    try:
        result = supabase.table("nca").insert(records).execute()
        db_records = result.model_dump(mode="python").get("data")
    except Exception as e:
        print("[!]\tError occured during db insertion")
        print(f"\t{e}")
        print("[*]\tRetrying...")
        _insert_records(release, records, batch_num)
    if db_records and len(db_records) == len(records):
        print(f"[*]\t{release["filename"]} (batch-{batch_num}) added")
    else:
        db_records = None
        print(
            f"[!]\tFailed adding {release["filename"]} (batch-{batch_num})")
        print("[*]\tRetrying...")
        _insert_records(release, records, batch_num)
    # print(db_records)
    return db_records


def delete_latest_nca_in_db(db_last_release: Dict):
    print("[INFO] Deleting 'latest' release and records...")
    supabase.table("release").delete().eq(
        "id", db_last_release["id"]).execute()
    print("[INFO] Finished Deleting 'latest'")


def load_nca_to_db(release: Dict, records: List[Dict]):
    db_release = _insert_release(release)
    print(f"[INFO] Inserting '{release["filename"]}' records...")
    if db_release:
        _add_release_id_to_records(db_release["id"], records)
        # print(records)
        db_records = []
        batch_num = 0
        for i in range(0, len(records), 100):
            batch_records = records[i:i+100]
            db_batch_records = _insert_records(
                release, batch_records, batch_num)
            if db_batch_records:
                db_records.extend(db_batch_records)
            time.sleep(1)
            batch_num += 1
    print(f"[INFO] Finished  inserting '{release["filename"]}' records")
