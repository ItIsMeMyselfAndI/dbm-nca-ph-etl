import time
from typing import Dict, List
from client import supabase


def _insert_release(release: Dict):
    result = supabase.table("release").insert(release).execute()
    data = result.model_dump(mode="python").get("data")
    if data:
        db_release = data[0]
        print(f"[*]\tNCA-{release['year']} release added")
    else:
        db_release = None
        print(f"[*]\tFailed adding NCA-{release['year']}")
    return db_release


def _add_release_id(release_id: str, data: List[Dict]):
    for row in data:
        row["release_id"] = release_id


def _insert_batch(table_name: str, release: Dict,
                  data: List[Dict], batch_num: int):
    max_attempt = 3
    i = 0
    while i < max_attempt:
        try:
            result = supabase.table(table_name).insert(data).execute()
            inserted_rows = result.model_dump(mode="python").get("data")

            if inserted_rows and len(inserted_rows) == len(data):
                print(f"[*]\tNCA-{release['year']
                                  } {table_name}s (batch-{batch_num}) added")
                return inserted_rows
            else:
                print(f"[!]\tFailed adding {release['filename']} {
                      table_name}s (batch-{batch_num})")
                print(f"[*]\tRetrying (attempt-{i+1})...")
                i += 1
                time.sleep(0.5)
                continue

        except Exception as e:
            print(f"[!]\tError occured during {table_name}s insertion")
            print(f"\t{e}")
            print(f"[*]\tRetrying (attempt-{i+1})...")
            i += 1
            time.sleep(0.5)
            continue

    print(f"[!]\tMax attempts ({max_attempt}) exceeded")
    print(f"[!]\tFailed to insert batch-{batch_num} {table_name}s")
    return None


def delete_latest_nca_in_db(db_last_release: Dict):
    print("[INFO] Deleting 'latest' release and records...")
    supabase.table("release").delete().eq(
        "id", db_last_release["id"]).execute()
    print("[INFO] Deleted 'latest' successfully")


def load_nca_to_db(release: Dict, records: List[Dict],
                   allocations: List[Dict]):
    print(f"[INFO] Inserting 'NCA-{release['year']}' data...")
    db_release = _insert_release(release)
    if db_release:
        batch_size = 100
        _add_release_id(db_release["id"], records)
        # insert records
        if len(records) > 0:
            for i in range(0, len(records), batch_size):
                batch_num = (i // batch_size) + 1
                _insert_batch("record", release,
                              records[i:i+batch_size], batch_num)
                time.sleep(0.5)
        else:
            print("[*]\tNo new records found")
        # insert allocations
        if len(allocations) > 0:
            for i in range(0, len(allocations), batch_size):
                batch_num = (i // batch_size) + 1
                _insert_batch("allocation", release,
                              allocations[i:i+batch_size], batch_num)
                time.sleep(0.5)
        else:
            print("[*]\tNo new allocations found")
    print("[INFO] Inserted data successfully")
