from __future__ import annotations
import os
import datetime
import hashlib
import json
import sys
import requests  # type: ignore
import gzip
import pickle
import sqlite3
import numpy as np
import numpy.typing as npt
import pandas as pd
import geopandas as gp

from zoneinfo import ZoneInfo
from collections import Counter
from shapely.geometry.base import BaseGeometry
from pathlib import Path

from typing import Any, Dict, List, Set, Hashable, Union, TypeAlias, cast

from basic_helpers.config_obs import PIPELINE_VERSION, OBS_MODE
from .observability_db import get_connection
from rd_observability.obs_conf import REL_FILE_TYPES, DB_PATH
from .obs_classes import Result

QualityMetrics = dict[str, int]
DictTypeMetrics = dict[str, dict[str, int] | QualityMetrics | int]
GeomMetrics = dict[str, int | dict[Hashable, int]]

DictMetrics = dict[str, int | str | DictTypeMetrics]
DfMetrics = dict[str, int | str | list[str] | tuple[int, int] | QualityMetrics]
GdfMetrics = dict[str, int | str | list[str] | tuple[int, int]  | QualityMetrics | GeomMetrics]
NumpyMetrics = dict[str, int | str | tuple[int, ...]]

NumpyArray: TypeAlias = npt.NDArray[Any]

Metrics: TypeAlias = GdfMetrics | DfMetrics | NumpyMetrics | DictMetrics | Result | dict[str, str | int]

ObsEventObj: TypeAlias = Union[Dict, List, Set, pd.DataFrame, gp.GeoDataFrame, Result, NumpyArray]

RecordVals: TypeAlias = tuple[str, str, str, str, str, str, str, str, int | None, str | None, int, int, str, str | None]

# ---------------------------
# Metrics Extraction Helpers
# ---------------------------

def df_quality_metrics(df: pd.DataFrame) -> QualityMetrics:
    # NaNs
    na_cnt: int = int(df.isna().sum().sum())

    # Infs (numeric columns only)
    num_df: pd.DataFrame = df.select_dtypes(include=[np.number])
    inf_cnt: int = int(np.isinf(num_df.values).sum()) if not num_df.empty else 0

    return {
        "nan_count": na_cnt,
        "inf_count": inf_cnt,
    }


def gdf_geom_metrics(gdf: gp.GeoDataFrame) -> GeomMetrics:
    geom: gp.GeoSeries = gdf.geometry

    empty_cnt: int = int(geom.is_empty.sum())
    valid_cnt: int = int(geom.is_valid.sum())
    invalid_cnt: int = len(gdf) - valid_cnt

    # Geometry type counts
    geom_types: dict[Hashable, int] = geom.geom_type.value_counts(dropna=False).to_dict()

    return {
        "geom_empty_count": empty_cnt,
        "geom_invalid_count": invalid_cnt,
        "geom_types": geom_types,
    }


def dict_type_metrics(d: dict[Hashable, Any], 
                      max_depth: int=3) -> DictTypeMetrics:
    counter: Counter = Counter()
    shapely_counter: Counter = Counter()
    file_type_counter: Counter = Counter()
    empty_cnt: int = 0
    invalid_cnt: int = 0

    def walk(x: dict[Hashable, Any], depth: int = 0):
        nonlocal empty_cnt, invalid_cnt
        if depth > max_depth:
            return
        
        # count THIS object
        counter[type(x).__name__] += 1

        if isinstance(x, dict):
            for v in x.values():
                walk(v, depth+1)

        elif isinstance(x, (list, tuple)):
            # tail shapely detection
            if x:
                last: BaseGeometry = x[-1]
                if isinstance(last, BaseGeometry):
                    gtype: str = last.geom_type
                    valid: bool = last.is_valid
                    empty: bool = last.is_empty

                    shapely_counter[gtype] += 1
                    #shapely_counter[f"{gtype}_invalid"] += int(not valid)
                    #shapely_counter[f"{gtype}_empty"] += int(empty)

                    invalid_cnt += int(not valid)
                    empty_cnt += int(empty)

        elif isinstance(x, str) and len(x) < 8:
            if x in REL_FILE_TYPES:
                file_type_counter[x] += 1
            #for v in x:
            #    walk(v, depth+1)

    walk(d)
    counter['dict'] -= 1  # Don't count the top-level dict / the object itself => subtract 1

    return {
        "value_type_counts": dict(counter),
        "geom_empty_count": empty_cnt,
        "geom_invalid_count": invalid_cnt,
        "geom_types": dict(shapely_counter),
        "file_types": dict(file_type_counter)
    }


# -------------------------
# Metrics Extractors
# -------------------------

def dict_metrics(d, max_depth=3) -> DictMetrics:
    def walk(x, depth=0):
        if not isinstance(x, dict) or depth > max_depth:
            return 0, depth
        count = len(x)
        maxd = depth
        for k, v in x.items():
            if k in [-1, 'cells']:
                continue
            c, d2 = walk(v, depth+1)
            count += c
            maxd = max(maxd, d2)
        return count, maxd

    if 'cells' in d:
        max_depth += 1

    if len(d) > 1_000_000:
        m = {
            "rel_cnt": len(d),
            "top_level_keys": len(d),
            "total_keys_recursive": len(d),
            "type": "dict",
        }
    else:
        total_keys, depth = walk(d)
        m = {
            "rel_cnt": len(d) if depth == 1 else total_keys - len(d),
            "top_level_keys": len(d),
            "total_keys_recursive": total_keys,
            "max_depth": depth,
            "type": "dict",
        }
        #m.update(dict_type_metrics(d, max_depth=max_depth))
        m2 = dict_type_metrics(d, max_depth=max_depth)
        m.update(m2)
        val_types = m2['value_type_counts']
        m["rel_cnt"] = len(d) if depth == 1 else (total_keys 
                                                  - cast(QualityMetrics, val_types).get("dict", 0) 
                                                  - cast(QualityMetrics, val_types).get("set", 0))

    return cast(DictMetrics, m)

def df_metrics(df: pd.DataFrame) -> DfMetrics:
    if len(df) == 0:
        print("DF COLUMNS", df.columns, df.shape)
        return {
        "rel_cnt": 0,
        "rows": len(df),
        "cols": len(df.columns),
        "shape": df.shape,
        "dims": 2,
        "columns": list(df.columns),
        # "memory_bytes": int(gdf.memory_usage(deep=True).sum()),
        "type": "DataFrame",
    }
    
    m = {
        "rel_cnt": len(df),
        "rows": len(df),
        "cols": len(df.columns),
        "shape": df.shape,
        "dims": 2,
        "columns": list(df.columns),
        # "memory_bytes": int(df.memory_usage(deep=True).sum()),
        "type": "DataFrame",
    }

    if len(df) > 25_000:
        m.update(df_quality_metrics(df.sample(n=min(len(df), 25_000), 
                                                    random_state=0)))
    else:
        m.update(df_quality_metrics(df))

    return cast(DfMetrics, m)


def gdf_metrics(gdf: gp.GeoDataFrame) -> GdfMetrics:
    if len(gdf) == 0:
        print("GDF COLUMNS", gdf.columns, gdf.shape)
        return {
        "rel_cnt": 0,
        "rows": len(gdf),
        "cols": len(gdf.columns),
        "shape": gdf.shape,
        "dims": 2,
        "columns": list(gdf.columns),
        # "memory_bytes": int(gdf.memory_usage(deep=True).sum()),
        "type": "GeoDataFrame",
    }

    m = {
        "rel_cnt": len(gdf),
        "rows": len(gdf),
        "cols": len(gdf.columns),
        "shape": gdf.shape,
        "dims": 2,
        "columns": list(gdf.columns),
        # "memory_bytes": int(gdf.memory_usage(deep=True).sum()),
        "type": "GeoDataFrame",
    }
    
    if len(gdf) > 25_000:
        m.update(df_quality_metrics(gdf.sample(n=min(len(gdf), 25_000), 
                                                     random_state=0)))
        m.update(gdf_geom_metrics(gdf.sample(n=min(len(gdf), 25_000), 
                                                   random_state=0)))
    else:
        m.update(df_quality_metrics(gdf))
        m.update(gdf_geom_metrics(gdf))

    return cast(GdfMetrics, m)

def np_metrics(arr: NumpyArray) -> NumpyMetrics:
    return cast(NumpyMetrics, 
    {
        "rel_cnt": arr.shape[0],
        "shape": arr.shape,
        "dims": len(arr.shape),
        "dtype": str(arr.dtype),
        # "size_bytes": arr.nbytes,
        "nan_count": int(np.isnan(arr).sum()) if np.issubdtype(arr.dtype, np.floating) else 0,
        "inf_count": int(np.isinf(arr).sum()) if np.issubdtype(arr.dtype, np.floating) else 0,
        "type": "Numpy Array",
    })


def extract_metrics(obj, max_depth=3) -> Metrics:
    if isinstance(obj, dict):
        return dict_metrics(obj, max_depth)
    if isinstance(obj, gp.GeoDataFrame):
        return gdf_metrics(obj)
    if isinstance(obj, pd.DataFrame):
        return df_metrics(obj)
    if isinstance(obj, np.ndarray):
        return np_metrics(obj)
    if isinstance(obj, Result):
        return obj.to_dict()
    
    try:
        len_obj = len(obj)
    except Exception as e:
        print("Error extract length", e)
        len_obj = -1
        pass

    return cast(dict[str, str | int], {"type": str(type(obj)), "rel_cnt": len_obj})


# -------------------------
# Fingerprints
# -------------------------

def file_fingerprint(fname: str | Path, chunk: int = 1024*1024) -> str:
    h = hashlib.sha256()
    with open(fname, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def semantic_fingerprint(obj: ObsEventObj) -> str | None:
    try:
        if isinstance(obj, dict):
            return hashlib.sha256(json.dumps(sorted(obj.keys())).encode()).hexdigest()
        if isinstance(obj, pd.DataFrame):
            return str(pd.util.hash_pandas_object(obj, index=True).sum())
        if isinstance(obj, Result):
            return hashlib.sha256(json.dumps(sorted(obj.to_dict().items())).encode()).hexdigest()
        if isinstance(obj, list) or isinstance(obj, set):
            return hashlib.sha256(json.dumps(sorted(list(obj)[:10] + list(obj)[-10:])).encode()).hexdigest()
    except Exception:
        pass
    return None

# Not recommended - RAM spikes
# def binary_fingerprint(obj):
#    return hashlib.sha256(pickle.dumps(obj)).hexdigest()


# ----------------------------------------
# Run ID as automatic versioning variant
# ----------------------------------------

def get_next_version(db_path: Path) -> int | None:
    try:
        conn: sqlite3.Connection | None = get_connection(db_path)
        if conn:
            cur: sqlite3.Cursor = conn.cursor()
            cur.execute("SELECT MAX(run_id) FROM artifacts")
            v: int = cur.fetchone()[0]
            conn.close()
            return (v or 0) + 1
    except sqlite3.OperationalError as e:
        print("SQLite Error - get_next_version", e)
    except Exception as e:
        print("Error - get_next_version", e)
    
    return None


# -------------------------
# SQLite / API Logger
# -------------------------

def record_event(obj: ObsEventObj, 
                 fname: str | Path, 
                 serializer: str, 
                 meta: dict[str, Any] | None = None, 
                 obs_api_url: str | None = None, 
                 db_path: Path = DB_PATH) -> None:
    if meta is None:
        return
    meta = meta or {}
    max_depth: int = 3
    if 'hierarchy' in str(fname).lower():
        max_depth = 6
    ts: str = datetime.datetime.now(datetime.timezone.utc).isoformat()[:19]
    print("RECORD_EVENT          ", ts, meta)

    size: int | None = os.path.getsize(fname) if os.path.exists(fname) else None
    
    metrics = extract_metrics(obj, max_depth) if obj is not None else {}
    #print("RECORD_EVENT - metrics", ts, meta)
    #fp = binary_fingerprint(obj) if obj is not None else None
    if os.path.exists(fname):
        #fp = file_fingerprint(fname)
        fp: str | None = "--"
    else:
        fp = semantic_fingerprint(obj) if obj is not None else None
    #script = inspect.stack()[-2].filename
    script: str | None = get_script_name()
    #print("RECORD_EVENT - fingerp", ts, meta)
    pipeline_version: int = PIPELINE_VERSION
    run_id: int = get_run_id()

    key: str = meta.get("key", "--")
    cell: str = meta.get("cell", "--")
    object_type: str = meta.get("object_type", "--")
    reg_key: str = meta.get("reg_type", "--")
    lvl: str = meta.get("lvl", "--")
    meta_run_id: int | None = meta.get("run_id")
    meta_script_name: str = meta.get("script_name", "")

    script = meta_script_name if meta_script_name else script
    run_id = meta_run_id if meta_run_id else run_id
    print("RECORD_EVENT - before ", ts, meta, db_path)
    if OBS_MODE == 'api' and obs_api_url:    
        payload: dict[str, int | str | Metrics | None] = dict(
            timestamp=ts,
            key=key,
            cell=cell,
            object_type=object_type,
            reg_key=reg_key,
            lvl=lvl,
            filename=str(fname),
            serializer=serializer,
            size_bytes=size,
            script_name=script,
            pipeline_version=pipeline_version,
            run_id=run_id,
            metrics_json=metrics,
            fingerprint=fp
        )

        try:
            requests.post(obs_api_url + "/gr_record_event", json=payload, timeout=2)
        except Exception as e:
            print("ERROR observability - record event", e)

        return None

    vals: RecordVals = (
        ts, key, cell, object_type, reg_key, lvl, str(fname), serializer, 
        size, script, pipeline_version, run_id, json.dumps(metrics), fp)
    
    try:
        conn: sqlite3.Connection | None = get_connection(db_path)
        if conn:
            conn.execute("""
                INSERT INTO artifacts (
                    timestamp, key, cell, object_type, reg_key, lvl, filename,
                    serializer, size_bytes, script_name, pipeline_version, run_id,
                    metrics_json, fingerprint
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, vals)
            conn.commit()
            conn.close()
            print("RECORD_EVENT - after  ", ts, meta)
    except sqlite3.OperationalError as e:
        print("SQLite Error - record_event", e)
        print("SQLite Error - record_event vals", vals)
    except Exception as e:
        print("Error - record_event", e)
        print("Error - record_event vals", vals)

    return None

# -------------------------
# Wrapped Save Functions
# -------------------------

def do_pickle(obj: Any, fname: str | Path, 
              meta: dict[str, int] | None = None, 
              obs_api_url: str | None = None) -> None:
    fname = str(fname)
    with open(fname, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)

    record_event(obj, fname, "pickle", db_path=DB_PATH, meta=meta, obs_api_url=obs_api_url)
    return None
    
    #gc.collect()


def do_gzip_pkl(obj: Any, fname: str | Path, 
                meta: dict[str, int] | None = None, 
                obs_api_url: str | None = None) -> None:
    fname = str(fname)
    with gzip.open(fname, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)

    record_event(obj, fname, "gzip_pickle", meta, obs_api_url)
    return None
    
    #gc.collect()


def save_to_csv(df: pd.DataFrame, fname: str | Path, 
                meta: dict[str, int] | None = None, 
                obs_api_url: str | None = None, **kwargs) -> None:
    fname = str(fname)
    df.to_csv(fname, **kwargs)

    record_event(df, fname, "csv", meta, obs_api_url)
    return None


def save_to_geojson(gdf: gp.GeoDataFrame, fname: str | Path, 
              meta: dict[str, int] | None = None, 
              obs_api_url: str | None = None, **kwargs) -> None:
    fname = str(fname)
    gdf.to_file(fname, driver="GeoJSON", index=True, **kwargs) #to_csv(fname, **kwargs)

    record_event(gdf, fname, "geojson", meta, obs_api_url)
    return None


# -----------------------------
# RUN_ID / SCRIPT_NAME
# -----------------------------

def set_run_params(script_name: str | None = None) -> None:
    curr_ts = datetime.datetime.now(ZoneInfo('Europe/Berlin')).isoformat()[:19]
    dt = int(curr_ts[2:10].replace('-', '')) - 2.6e5
    tm = int(curr_ts[11:19].replace(':', ''))
    os.environ['OBS_RUN_ID'] = str(int(dt*1e6+tm))

    if script_name:
        os.environ['OBS_SCRIPT_NAME'] = script_name
    else:
        os.environ['OBS_SCRIPT_NAME'] = os.path.abspath(sys.argv[0])

def get_run_id() -> int:
    return int(os.environ['OBS_RUN_ID'])

def get_script_path() -> str:
    return os.environ.get("OBS_SCRIPT_NAME", "UNKNOWN")

def get_script_name() -> str:
    return os.path.basename(os.environ.get("OBS_SCRIPT_NAME", "UNKNOWN"))