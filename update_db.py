import os
import sys
from glob import glob
from typing import Any, Dict, Tuple
import terracotta
from terracotta import get_driver
from joblib import Parallel, delayed
from rich import print

import config

DB_PATH = "db.sqlite"


def filename_to_keys(filename: str) -> Dict[str, str]:
    return {"file": os.path.basename(filename)[:-4]}


def create_schema(driver: terracotta.drivers.base.TerracottaDriver) -> None:
    driver.create(keys=["file"], key_descriptions={"file": "The filename of the dataset"})


def process_file(filepath):
    print(f"Computing metadata for {filepath}")
    try:
        # Create a fresh driver inside the worker to avoid pickling issues
        driver = get_driver(DB_PATH)
        metadata = driver.compute_metadata(filepath)
        key = os.path.basename(filepath)[:-4]
        return key, filepath, metadata
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return None


def refresh_datasets(driver: terracotta.drivers.base.TerracottaDriver, force: bool = False):
    # Use full paths for comparison
    new_files = {os.path.normpath(f) for f in glob("data/*.tif")}

    existing_datasets = driver.get_datasets()
    # existing_datasets.items() returns ((key1, ...), path)
    # so item[1] is the path
    existing = {os.path.normpath(path) for path in existing_datasets.values()}

    if force:
        files_to_add = new_files
    else:
        files_to_add = new_files - existing
    
    files_to_remove = existing - new_files

    print(f"Adding/Updating: {len(files_to_add)} files")
    print(f"Removing: {len(files_to_remove)} files")

    if not files_to_add and not files_to_remove:
        return

    results = Parallel(n_jobs=4)(
        delayed(process_file)(f) for f in files_to_add
    )

    for result in results:
        if result:
            key, filepath, metadata = result
            print(f"Inserting {key} into database")
            driver.insert({"file": key}, filepath, metadata=metadata)

    for path in files_to_remove:
        key = os.path.basename(path)[:-4]
        print(f"Deleting {key} from database")
        driver.delete({"file": key})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--nuke", dest="nuke", action="store_true")
    parser.add_argument("--force", dest="force", action="store_true")
    args = parser.parse_args()
    print(args)

    if args.nuke and os.path.isfile(DB_PATH):
        os.remove(DB_PATH)

    driver = get_driver(DB_PATH)

    if not os.path.isfile(DB_PATH):
        create_schema(driver)
    else:
        # Check if we need to migrate or if it's broken
        try:
            driver.get_keys()
        except Exception:
            print("Database schema seems invalid. Nuking and recreating...")
            os.remove(DB_PATH)
            driver = get_driver(DB_PATH)
            create_schema(driver)

    refresh_datasets(driver, force=args.force)
