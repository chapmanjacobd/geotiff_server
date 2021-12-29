import os
import sys
from glob import glob
from IPython.core import ultratb
from rich import print
import config


sys.excepthook = ultratb.FormattedTB(color_scheme="Neutral", call_pdb=1)


def filename_to_keys(filename):
    return os.path.basename(filename)[:-4]


def refresh_datasets(db):
    with db.connect():
        new_files = {filename_to_keys(f) for f in glob("data/*.tif")}
        existing = set(map(lambda x: x[0], db.get_datasets().keys()))

        keys_to_add = new_files - existing
        keys_to_remove = existing - new_files

        print("Adding")
        print(keys_to_add)
        print("Removing")
        print(keys_to_remove)

        if not keys_to_add and not keys_to_remove:
            return

        for key in keys_to_add:
            print(key)
            db.insert([key], f"data/{key}.tif")

        for key in keys_to_remove:
            db.delete([key])


if __name__ == "__main__":
    import terracotta
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--nuke", dest="nuke", action="store_true")
    args = parser.parse_args()
    print(args)

    DB_PATH = "db.sqlite"

    db = terracotta.get_driver(DB_PATH, provider="sqlite")

    if args.nuke and os.path.isfile(DB_PATH):
        os.remove(DB_PATH)
    if not os.path.isfile(DB_PATH):
        db.create(keys=["file"])

    refresh_datasets(db)
