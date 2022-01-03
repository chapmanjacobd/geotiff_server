import json
import os
import sqlite3
import sys
from glob import glob
from typing import Any, Dict, List, Mapping, Sequence, Tuple, Union, cast
import numpy as np
from IPython.core import ultratb
from joblib import Parallel, delayed
from rasterio.dtypes import get_minimum_dtype
from rich import inspect, print
from shapely import geometry
from terracotta import __version__

import config

sys.excepthook = ultratb.FormattedTB(color_scheme="Neutral", call_pdb=1)


def filename_to_keys(filename):
    return os.path.basename(filename)[:-4]


def create_schema(DB_PATH) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS terracotta (version VARCHAR[255])")
    conn.execute("INSERT INTO terracotta VALUES (?)", [str(__version__)])  # "0.7.4"

    conn.execute(
        "CREATE TABLE IF NOT EXISTS keys (key VARCHAR[256], description VARCHAR[max])"
    )
    conn.executemany("INSERT INTO keys VALUES (?, ?)", [("file", "")])

    conn.execute(
        "CREATE TABLE IF NOT EXISTS datasets (file VARCHAR[256], filepath VARCHAR[8000], PRIMARY KEY(file));"
    )
    conn.execute(
        (
            "CREATE TABLE IF NOT EXISTS metadata (file VARCHAR[256], bounds_north REAL, bounds_east REAL,"
            " bounds_south REAL, bounds_west REAL, convex_hull VARCHAR[max], valid_percentage REAL,"
            " min REAL, max REAL, mean REAL, stdev REAL, percentiles BLOB, metadata VARCHAR[max], PRIMARY KEY (file));"
        )
    )
    conn.commit()
    conn.close()


def _key_dict_to_sequence(keys: Union[Mapping[str, Any], Sequence[Any]]) -> List[Any]:
    """Convert {key_name: key_value} to [key_value] with the correct key order."""
    try:
        keys_as_mapping = cast(Mapping[str, Any], keys)
        return [keys_as_mapping[key] for key in ["file"]]
    except TypeError:  # not a mapping
        return list(keys)
    except KeyError as exc:
        raise Exception("Encountered unknown key") from exc


def insert(
    conn,
    keys: Union[Sequence[str], Mapping[str, str]],
    filepath: str,
    metadata: Mapping[str, Any] = None,
) -> None:
    def _encode_data(decoded: Mapping[str, Any]) -> Dict[str, Any]:
        """Transform from internal format to database representation"""
        encoded = {
            "bounds_north": decoded["bounds"][0],
            "bounds_east": decoded["bounds"][1],
            "bounds_south": decoded["bounds"][2],
            "bounds_west": decoded["bounds"][3],
            "convex_hull": json.dumps(decoded["convex_hull"]),
            "valid_percentage": decoded["valid_percentage"],
            "min": decoded["range"][0],
            "max": decoded["range"][1],
            "mean": decoded["mean"],
            "stdev": decoded["stdev"],
            "percentiles": np.array(decoded["percentiles"], dtype="float32").tobytes(),
            "metadata": "",
        }
        return encoded

    keys = _key_dict_to_sequence(keys)
    template_string = ", ".join(["?"] * (len(keys) + 1))
    conn.execute(
        f"INSERT OR REPLACE INTO datasets VALUES ({template_string})",
        [*keys, filepath],
    )

    encoded_data = _encode_data(metadata)
    row_keys, row_values = zip(*encoded_data.items())
    template_string = ", ".join(["?"] * (len(keys) + len(row_values)))
    conn.execute(
        f"INSERT OR REPLACE INTO metadata (file, "
        f'{", ".join(row_keys)}) VALUES ({template_string})',
        [*keys, *row_values],
    )


def delete(conn, keys: Union[Sequence[str], Mapping[str, str]]) -> None:
    key_names = ["file"]

    if len(keys) != len(key_names):
        raise Exception(f"Got wrong number of keys (available keys: {key_names})")

    keys = _key_dict_to_sequence(keys)

    where_string = " AND ".join([f"{key}=?" for key in key_names])
    conn.execute(f"DELETE FROM datasets WHERE {where_string}", keys)
    conn.execute(f"DELETE FROM metadata WHERE {where_string}", keys)


def compute_metadata(key, filepath) -> Dict[str, Any]:
    import rasterio
    from rasterio import warp

    with rasterio.open(filepath) as raster_data:
        bounds = warp.transform_bounds(
            raster_data.crs, "epsg:4326", *raster_data.bounds, densify_pts=21
        )
        x_resolution, _y = raster_data.res

        band1_data = raster_data.read(1, masked=True)
        total_px_count = band1_data.size

        band1_data = np.ma.masked_equal(band1_data, raster_data.nodata or 0, copy=False)
        band1_data = np.ma.masked_invalid(band1_data, copy=False)

        valid_data = band1_data.compressed()

        valid_px_count = np.ma.count(valid_data)
        if valid_px_count == 0:
            print("No valid pixels found!")
            print(filepath)
            os.unlink(filepath)
            return

        (unique, counts) = np.unique(valid_data, return_counts=True)
        count_index_sort = np.argsort(-counts)
        top_100_frequent_values = unique[count_index_sort][:100]

        try:
            negative_px_count = counts[np.where(unique < 0)].sum()
        except IndexError:
            negative_px_count = 0

        # no masked entries -> convex hull == dataset bounds
        w, s, e, n = raster_data.bounds
        convex_hull = geometry.Polygon([(w, s), (e, s), (e, n), (w, n)])

        convex_hull_wgs = warp.transform_geom(
            raster_data.crs, "epsg:4326", geometry.mapping(convex_hull)
        )

        return key, {
            "valid_percentage": valid_data.size / band1_data.size * 100,
            "range": (float(valid_data.min()), float(valid_data.max())),
            "mean": float(valid_data.mean()),
            "stdev": float(valid_data.std()),
            "percentiles": np.percentile(valid_data, np.arange(1, 100)),
            "convex_hull": convex_hull_wgs,
            "bounds": bounds,
            "dtype": str(raster_data.dtypes[0]),
            "min_scalar_type": str(get_minimum_dtype(valid_data)),
            "pixel_size": x_resolution,
            "median": float(np.ma.median(valid_data)),
            "negative_px_count": negative_px_count,
            "valid_px_count": valid_px_count,
            "total_px_count": total_px_count,
            "nodata_value": raster_data.nodata,
            "top_100_frequent_values": top_100_frequent_values.tolist(),
        }


def refresh_datasets(conn):
    def get_datasets(conn) -> Dict[Tuple[str, ...], str]:
        rows = conn.execute(f"SELECT * FROM datasets order by file")

        def keytuple(row: Dict[str, Any]) -> Tuple[str, ...]:
            return tuple(row[key] for key in ["file"])

        return {keytuple(row): row["filepath"] for row in rows}

    new_files = {filename_to_keys(f) for f in glob("data/*.tif")[:1]}
    existing = set(map(lambda x: x[0], get_datasets(conn).keys()))

    keys_to_add = new_files - existing
    keys_to_remove = existing - new_files

    print("Adding")
    print(keys_to_add)
    print("Removing")
    print(keys_to_remove)

    if not keys_to_add and not keys_to_remove:
        return

    new_metadata = Parallel(n_jobs=3)(
        delayed(compute_metadata)(key, f"data/{key}.tif") for key in keys_to_add
    )
    new_metadata = list(filter(lambda x: x != None, new_metadata))
    print(new_metadata)
    for key, metadata in new_metadata:
        insert(conn, [key], f"data/{key}.tif", metadata=metadata)

    for key in keys_to_remove:
        delete([key])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--nuke", dest="nuke", action="store_true")
    args = parser.parse_args()
    print(args)

    DB_PATH = "db.sqlite"
    if args.nuke and os.path.isfile(DB_PATH):
        os.remove(DB_PATH)

    if not os.path.isfile(DB_PATH):
        create_schema(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    refresh_datasets(conn)
    conn.commit()
    conn.close()
