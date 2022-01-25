import json
import os
import sqlite3
import numpy as np
from rich import print


def _decode_data(encoded):
    """Transform from database format to internal representation"""
    decoded = {
        "file": encoded["file"],
        "bounds": tuple(
            [encoded[f"bounds_{d}"] for d in ("north", "east", "south", "west")]
        ),
        "convex_hull": json.loads(encoded["convex_hull"]),
        "valid_percentage": encoded["valid_percentage"],
        "range": (encoded["min"], encoded["max"]),
        "mean": encoded["mean"],
        "stdev": encoded["stdev"],
        "percentiles": np.frombuffer(encoded["percentiles"], dtype="float32").tolist(),
    }
    return decoded


def get_metadata(conn):
    rows = [dict(r) for r in conn.execute(f"SELECT * FROM metadata").fetchall()]

    return list(map(_decode_data, rows))


if __name__ == "__main__":
    DB_PATH = "db.sqlite"

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    d = get_metadata(conn)
    print(d)
