import os
import json
import requests  # type: ignore
import datetime
import sqlite3
import pandas as pd
import geopandas as gp
#import numpy as np
from pathlib import Path
from typing import Any, Callable, TypeAlias, Union, cast

#from .obs_conf import OBS_API_URL
from .observability_db import get_connection
from .observability import (extract_metrics, semantic_fingerprint, get_script_name, OBS_MODE, 
                           set_run_params, get_run_id) # get_script_path
from .obs_baseline_helper import get_key, get_cell, get_object_type, get_geocode_geom_data # get_reg_key, 
from .obs_baseline_conf import BASELINE_DICT, BaselineConfDict, MetadataTriplet
from .observability import ObsEventObj

from basic_helpers.types_osm import TagsetDict, RevTagsetDict
from basic_helpers.file_helper import do_unpickle, do_ungzip_pkl
from basic_helpers.config import data_base_path
#from basic_helpers.config_obs import PIPELINE_VERSION
from basic_helpers.config_reg_keys import REG_KEYS

from lvl2_env_api.env_config import get_w_by_node_tagset_id
from lvl2_env_api.read_env_data import get_data_gdf, get_env_data_path, get_modified_env_data_path #, D_BY_TAGSET_ID

'''
try:
    sys.path.append('C:/05_Python/osm')
    sys.path.append('C:/05_Python/kaggle_env_api')
    #from file_helper import do_unpickle, do_ungzip_pkl
    #from config_helper import data_base_path, PIPELINE_VERSION, REG_KEYS
    from read_env_data import get_data_gdf, get_env_data_path, get_modified_env_data_path, D_BY_TAGSET_ID
    from env_config import get_w_by_node_tagset_id
except:
    try:
        sys.path.append("/kaggle/input/geom-helpers")
        sys.path.append("../basic_helpers")
        sys.path.append("../latlon_code")
        sys.path.append("../distance_helper")
        sys.path.append("../shape2code")
        sys.path.append("../kaggle-env-api")
        #from config_helper import PIPELINE_VERSION, OBS_MODE, REG_KEYS
        #from file_helper import do_unpickle, do_ungzip_pkl
        from read_env_data import get_data_gdf, get_env_data_path, get_modified_env_data_path, D_BY_TAGSET_ID
        from env_config import get_w_by_node_tagset_id
    except Exception as e:
        print("Could not load", e)
'''

# Define the recursive type alias
# A node is a mapping where the value is either another Tree or a string (extension)
Tree: TypeAlias = dict[str, Union["Tree", str]]

lvl: str = '1'
db_path: Path = Path("C:/Workspaces/RD_OBSERVABILITY/BASELINE.sqlite")

#if len(D_BY_TAGSET_ID) == 0:
try:
    NODES_TAGSET_DICT: TagsetDict = do_ungzip_pkl(f"{data_base_path}/lvl1/NODES_TAGSETS_DICT.gzip")
except Exception:
    NODES_TAGSET_DICT: TagsetDict = do_unpickle("/kaggle/input/all-tagset-dicts/NODES_TAGSETS_DICT.pkl")  # type: ignore

REV_NODES_TAGSET_DICT: RevTagsetDict = {v: k for k, v in NODES_TAGSET_DICT.items()}
_ = get_w_by_node_tagset_id(REV_NODES_TAGSET_DICT)

def record_event_baseline(obj: ObsEventObj, 
                          fname: str | Path, 
                          serializer: str, 
                          lvl: str = '1', 
                          meta: dict[str, Any] | None = None, 
                          obs_api_url: str | None = None, 
                          db_path: str | Path | None = db_path) -> None:
    if meta is None:
        return
    meta = meta or {}
    max_depth = 3
    if 'hierarchy' in str(fname).lower():
        max_depth = 6

    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()[:19]
    #print("RECORD_EVENT BL          ", ts, meta)

    size = os.path.getsize(fname) if os.path.exists(fname) else None
    metrics = extract_metrics(obj, max_depth) if obj is not None else {}
    #print("RECORD_EVENT BL - metrics", type(obj), ts, meta)
    #fp = binary_fingerprint(obj) if obj is not None else None
    if os.path.exists(fname):
        #fp = file_fingerprint(fname)
        fp: str | None = "--"
    else:
        fp = semantic_fingerprint(obj) if obj is not None else "--"
    #script = inspect.stack()[-2].filename
    script = get_script_name()
    #print("RECORD_EVENT BL - fingerp", ts, meta)
    pipeline_version = "0"
    run_id = 0

    key = meta.get("key", "--")
    cell = meta.get("cell", "--")
    object_type = meta.get("object_type", "--")
    reg_key = meta.get("reg_type", "--")
    #if reg_key == '--' and 'reg'
    lvl = meta.get("lvl", "--") if 'lvl' in meta else lvl
    meta_run_id = meta.get("run_id")
    meta_script_name = meta.get("script_name")

    script = meta_script_name if meta_script_name else script
    run_id = meta_run_id if meta_run_id else run_id
    print("RECORD_EVENT BL - before ", ts, OBS_MODE, meta)
    if OBS_MODE == 'api' and obs_api_url:    
        payload = dict(timestamp=ts, key=key, cell=cell, object_type=object_type, 
                       reg_key=reg_key, lvl=lvl, size_bytes=size, 
                       filename=str(fname).replace(data_base_path[:-6], ""), 
                       serializer=serializer, script_name=script, 
                       pipeline_version=pipeline_version, run_id=run_id, 
                       metrics_json=metrics, fingerprint=fp)

        try:
            requests.post(obs_api_url + "/gr_record_event", json=payload, timeout=2)
        except Exception as e:
            print("ERROR observability - record event", e)

        return
    
    vals = (ts, key, cell, object_type, reg_key, lvl, 
            str(fname).replace(data_base_path[:-6], ""), 
            serializer, size, script, pipeline_version, 
            run_id, json.dumps(metrics), fp)
    #print("RECORD_EVENT BL", vals)

    try:
        assert db_path
        conn = get_connection(Path(db_path))   # get_connection already handles init_db() if DB doesn't exist
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
    except sqlite3.OperationalError as e:
        print("SQLite Error - record_event_baseline", e)
        print("SQLite Error - record_event_baseline vals", vals)
    except Exception as e:
        print("Error - record_event_baseline", e)
        print("Error - record_event_baseline vals", vals)
    #print("RECORD_EVENT BL - after  ", ts, meta)


def process_file_ls(file_ls: list[str], 
                    meta_conf: list[None | str | Callable], 
                    fpath: str | None, otype1: str, otype2: str, ftype: str, lvl: str | None, 
                    db_path: Path | str | None, base_path: str = data_base_path, 
                    obs_api_url: str | None = None, test_mode: bool = False) -> None:
    
    get_key, get_cell, get_reg_key = meta_conf
    data_type = None
    this_data_path = f"{base_path}/{fpath}" if os.path.exists(f"{base_path}/{fpath}") else fpath
    for fname in file_ls:
        key = get_key if (isinstance(get_key, str) 
                            or get_key is None) else get_key(fname)
        cell = get_cell if (isinstance(get_cell, str) 
                            or get_cell is None) else get_cell(fname, ftype, otype1, otype2)
        reg_key = get_reg_key if (isinstance(get_reg_key, str) 
                                    or get_reg_key is None) else get_reg_key(fpath, fname, otype2)
        if "csv" in fname:
            print("CSV", ftype, f"{base_path}/{fpath}/{fname}  ||", fpath, otype1, otype2)
        if "txt" in fname:
            print("TXT", ftype, f"{base_path}/{fpath}/{fname}  ||", fpath, otype1, otype2, ftype)
        if "pkl" in fname:
            print("PKL", ftype, f"{base_path}/{fpath}/{fname}  ||", fpath, otype1, otype2)
        if "gzip" in fname:
            print("GZIP", ftype, f"{base_path}/{fpath}/{fname}  ||", fpath, otype1, otype2)
        if ".zip" in fname:
            print("ZIP", ftype, f"{base_path}/{fpath}/{fname}  ||", fpath, otype1, otype2)
        if ftype == "df":
            if fname.endswith(".csv"):
                if lvl=='2' and this_data_path and not os.path.exists(this_data_path) \
                    and os.path.exists(f"{base_path}/{this_data_path}-{otype2}"):
                    
                    this_data_path = f"{base_path}/{this_data_path}-{otype2}"
                    print("CSV 2", this_data_path)

                try:
                    print("trying to read_csv", f"{this_data_path}/{fname}")
                    obj = pd.read_csv(f"{this_data_path}/{fname}", sep="\t", 
                                        index_col=0, low_memory=False)
                except Exception as e:
                    print("PROCESS_FILE_LS - INVALID conf 1", otype1, otype2, ftype, fname, e)
                    continue
            else:
                print("PROCESS_FILE_LS - INVALID file type 1", otype1, otype2, ftype, fname)

        elif ftype == "txt":
            if fname.endswith(".txt") and fpath and reg_key and cell:
                try:
                    print("trying to get_data_gdf", cell, fpath, reg_key, base_path)
                    #data_type = 'ways' if otype1 == 'ways' and otype2.upper() in REG_KEYS else fpath.split("/")[0]
                    data_type = 'ways' if otype1 == 'ways' and fpath.upper() in REG_KEYS else fpath.split("/")[0]
                    if otype1 == 'add-features':
                        data_type = otype2
                    #      data_type   dach add-features lighthouses dach
                    print("data_type", data_type, otype1, otype2, fpath, reg_key)
                    env_data_path = get_env_data_path(cell, data_type, reg_key.lower(), base_path)
                    mod_env_data_path = get_modified_env_data_path(env_data_path, base_path)[0]
                    this_data_path_comps = mod_env_data_path.replace(base_path, "")[1:].split("/")
                    this_data_path = this_data_path_comps[0] if data_type != 'ways' else "/".join(this_data_path_comps[:2])
                    if otype1 == 'add-features':
                        this_data_path = f"{base_path}/{this_data_path}/{otype2}"
                    elif lvl=='2' and otype1 == 'ways':
                        this_data_path = f"{base_path}/{this_data_path}"
                    elif lvl=='2' and otype1 == 'admin' and len(fpath) > 5 and len(fpath) < 8 and fpath[:5] == 'admin':
                        this_data_path = f"{base_path}/{this_data_path}"
                    print("get_env_data_path     ", env_data_path)
                    print("get_env_data_path mod ", mod_env_data_path)
                    print("this_data_path        ", this_data_path)
                    obj = get_data_gdf(cell, data_type, region=reg_key.lower(), base_path=base_path)
                    if lvl=='2' and otype1 == 'ways' and fpath=='guideposts' and obj is not None:
                        if 'Destination' in fname:
                            obj = obj[1]
                        else:
                            obj = obj[0]
                        
                    print("OBJ TYPE", type(obj))
                except Exception as e:
                    print("PROCESS_FILE_LS - INVALID conf 1.2", otype1, otype2, ftype, fname)
                    print("PROCESS_FILE_LS - INVALID conf 1.2", e)
            else:
                print("PROCESS_FILE_LS - INVALID file type 1.2", otype1, otype2, ftype, fname)
        
        elif ftype == "pkl":
            if fname.endswith(".pkl"):
                try:
                    if lvl=='1':
                        if otype1 != 'relats':
                            print("trying to unpickle 1", f"{this_data_path}/{fname}")
                            obj = do_unpickle(f"{this_data_path}/{fname}")
                        elif os.path.exists(f"{this_data_path}/{fname}"):
                            obj = do_unpickle(f"{this_data_path}/{fname}")
                        else:
                            fname = fname.replace(".pkl", ".gzip")
                            obj = do_ungzip_pkl(f"{this_data_path}/{fname}")
                    elif lvl=='2':
                        if not os.path.exists(f"{this_data_path}/{fname}") and fpath:
                            print("LVL2.0")
                            if os.path.exists(f"{base_path}/{fpath}-{otype2}"):
                                print("LVL2.0.1")
                                this_data_path = f"{base_path}/{fpath}-{otype2}"
                            elif os.path.exists(f"{base_path}/{fpath}/{otype2}"):
                                print("LVL2.0.2")
                                this_data_path = f"{base_path}/{fpath}/{otype2}"
                            elif os.path.exists(f"{base_path}/{fpath}"):
                                print("LVL2.0.3")
                                this_data_path = f"{base_path}/{fpath}"
                        print("LVL2 this_data_path", this_data_path)
                        obj = do_unpickle(f"{this_data_path}/{fname}")
                    else:
                        fname = fname.replace(".pkl", ".gzip")
                        obj = do_ungzip_pkl(f"{this_data_path}/{fname}")

                except Exception as e:
                    print("PROCESS_FILE_LS - INVALID conf 2", otype1, otype2, ftype, fname, e)

            else:
                print("PROCESS_FILE_LS - INVALID file type 2", otype1, otype2, ftype, fname)

        elif ftype == "gzip":
            if fname.endswith(".gzip"):
                try:
                    if lvl=='2':
                        if not os.path.exists(f"{this_data_path}/{fname}") and fpath:
                            print("LVL3.0", fpath)
                            if os.path.exists(f"{base_path}/{fpath}-{otype2}"):
                                print("LVL3.0.1")
                                this_data_path = f"{base_path}/{fpath}-{otype2}"
                            elif os.path.exists(f"{base_path}/{fpath}/{otype2}"):
                                print("LVL3.0.2")
                                this_data_path = f"{base_path}/{fpath}/{otype2}"
                            elif os.path.exists(f"{base_path}/{otype1}-{fpath}/{otype2}"):
                                print("LVL3.0.3")
                                this_data_path = f"{base_path}/{otype1}-{fpath}/{otype2}"
                            elif os.path.exists(f"{base_path}/{fpath}"):
                                print("LVL3.0.4")
                                this_data_path = f"{base_path}/{fpath}"
                            elif os.path.exists(f"{base_path}/{otype1}-{fpath}"):
                                print("LVL3.0.5")
                                this_data_path = f"{base_path}/{otype1}-{fpath}"
                        print("LVL3 this_data_path", this_data_path)
                    obj = do_ungzip_pkl(f"{this_data_path}/{fname}")
                    if lvl=='2' and otype1 == 'rd-geocode' and isinstance(obj, gp.GeoDataFrame):
                        obj = pd.DataFrame(obj)
                    print("OBJ TYPE", type(obj))
                except Exception as e:
                    print("PROCESS_FILE_LS - INVALID conf 3", otype1, otype2, ftype, fname, e)
            else:
                print("PROCESS_FILE_LS - INVALID file type 3", otype1, otype2, ftype, fname)
        elif ftype == "zip / txt" and isinstance(cell, str):
            if fname.endswith(".zip") and otype1 == 'rd-geocode' and reg_key in REG_KEYS:
                try:
                    obj = get_geocode_geom_data(cell, reg_key=reg_key, base_path=base_path)
                    this_data_path = f"{base_path}/{otype1}-{fpath}/{otype2}"
                    print("OBJ TYPE", type(obj), "this_data_path", this_data_path)
                except Exception as e:
                    print("PROCESS_FILE_LS - INVALID conf 4", otype1, otype2, ftype, fname, e)
            else:
                print("PROCESS_FILE_LS - INVALID file type 4", otype1, otype2, ftype, fname)
        
        print("META OBJ_TYPE", otype1, otype2, fpath, "|", get_object_type(otype1, otype2, fpath, lvl))
        meta = {'key': key, 'cell': cell, 
                'reg_type': reg_key.upper() if isinstance(reg_key, str) else None, 
                'object_type': get_object_type(otype1, 
                                               otype2, 
                                               fpath if data_type != 'ways' else (
                                                   fpath.split("/")[-1] 
                                                   if isinstance(fpath, str) else None), 
                                               lvl),
                "lvl": lvl}
        
        if OBS_MODE == 'api':
            if 'run_id' not in meta:
                meta['run_id'] = get_run_id()
            if 'script_name' not in meta:
                meta['script_name'] = get_script_name()
        #print("meta ", meta)
        #print("seri ", f"baseline {ftype}")
        print("fname", f"{this_data_path}/{fname}", os.path.exists(f"{this_data_path}/{fname}"))
        #print("fname", f"{fpath}/{fname}", os.path.exists(f"{fpath}/{fname}"))
        
        if obj is not None:
            record_event_baseline(cast(ObsEventObj, obj), serializer=f"baseline {ftype}", 
                                  fname=f"{this_data_path}/{fname}", 
                                  obs_api_url=obs_api_url, lvl='1', meta=meta, 
                                  db_path=Path(db_path) if db_path else None)
        if test_mode:
            break


def process_subdirs(fpath: str, meta_conf: list[None | str | Callable], 
                    otype1: str, otype2: str, ftype: str, lvl: str | None, 
                    db_path: Path | str | None, base_path: str = data_base_path, 
                    obs_api_url: str | None = None, filter_by: str | None=None, 
                    test_mode: bool = False) -> None:
    if lvl == '1':
        otype_path = f"{base_path}/{fpath}/_{otype2.upper()}"
    elif lvl == '2' and otype1 == 'rd-geocode':
        otype_path = f"{base_path}/{otype1}-{fpath}/{otype2}"
    elif lvl == '2' and otype1 == 'add-features':
        if otype2 != 'tagset':
            otype_path = f"{base_path}/{otype1}-{fpath}/{otype2}"
        else:
            otype_path = f"{base_path}/{otype1}-{fpath}"
    elif lvl == '2' and otype1 != 'admin':
        otype_path = f"{base_path}/{otype1}-{fpath}-{otype2.lower()}"
    else:
        otype_path = f"{base_path}/{fpath}-{otype2.lower()}"

    if os.path.exists(otype_path):
        for f in sorted(os.listdir(otype_path)):
            if filter_by and isinstance(filter_by, str):
                if not f.endswith(filter_by):
                    continue
            if not os.path.isdir(f"{otype_path}/{f}"):
                print("A", [f])
                try:
                    this_meta_conf = cast(MetadataTriplet, 
                                          [cast(Callable, get_key)] + meta_conf[1:])
                    if lvl == '2':
                        this_meta_conf[0] = None
                        this_meta_conf[1] = get_cell
                        if f.endswith(".txt"):
                            ftype = "txt"
                    print("A this_meta_conf", this_meta_conf)
                    process_file_ls([f], this_meta_conf, 
                                    f"{fpath}/_{otype2.upper()}" if lvl=='1' else fpath, 
                                    otype1, otype2, ftype, lvl, db_path, 
                                    base_path, obs_api_url, test_mode)
                except Exception as e:
                    print("PROCESS_FILE_LS - INVALID conf A", otype1, otype2, ftype, [f])
                    print("   - ", e)
                #break
            else:
                this_fpath = f"{otype_path}/{f}"
                file_ls = [fname for fname in os.listdir(this_fpath) 
                           if not os.path.isdir(f"{this_fpath}/{fname}") and fname.endswith(ftype)]
                print("B", this_fpath, file_ls[:3])
                try:
                    process_file_ls(file_ls, [meta_conf[0], get_cell, meta_conf[2]], 
                                    f"{fpath}/_{otype2.upper()}/{f}" if lvl=='1' else fpath, 
                                    otype1, otype2, ftype, lvl, db_path, 
                                    base_path, obs_api_url, test_mode)
                except Exception as e:
                    print("PROCESS_FILE_LS - INVALID conf B", otype1, otype2, ftype, file_ls[:3])
                    print("   - ", e)
                #break
            if test_mode:
                break
    else:
        print("otype_path does not exist", otype_path)


def process_way_dirs(fpath: str, meta_conf: list[None | str | Callable], 
                     otype1: str, otype2: str, ftype: str, lvl: str | None, 
                     db_path: Path | str | None, base_path: str = data_base_path, 
                     obs_api_url: str | None = None, test_mode: bool = False) -> None:

    if lvl == '1':
        otype_path = f"{base_path}/{fpath}/{otype2.upper()}"
    elif fpath.upper() in REG_KEYS:
        otype_path = f"{base_path}/{otype1}-{fpath.lower()}/{otype2}"
    else:
        otype_path = f"{base_path}/{otype1}-{otype2.lower()}/{fpath}"

    #otype_path = f"{base_path}/{fpath}/{otype2}"
    print("process_way_dirs / base_path ", base_path)
    print("process_way_dirs / otype_path", otype_path)
    if os.path.exists(otype_path):
        for f in sorted(os.listdir(otype_path)):
            if not os.path.isdir(f"{otype_path}/{f}"):
                print("A", [f], [meta_conf[0], get_cell, meta_conf[2]], lvl)
                try:
                    if lvl == '2' and f.endswith(".txt"):
                        ftype = "txt"
                    process_file_ls([f], [meta_conf[0], get_cell, meta_conf[2]], 
                                    f"{fpath}/{otype2}" if lvl == '1' else (fpath if fpath.upper() in REG_KEYS else f"{otype1}-{otype2.lower()}/{fpath}"), 
                                    otype1, otype2, ftype, lvl, 
                                    db_path, base_path, obs_api_url, test_mode)
                except Exception as e:
                    print("PROCESS_FILE_LS - INVALID conf A", otype1, otype2, ftype, [f])
                    print("   - ", e)
                #break
            else:
                this_fpath = f"{otype_path}/{f}"
                file_ls = [fname for fname in os.listdir(this_fpath) 
                           if not os.path.isdir(f"{this_fpath}/{fname}") and fname.endswith(ftype)]
                print("B", this_fpath, file_ls[:3])
                try:
                    process_file_ls(file_ls, [meta_conf[0], get_cell, meta_conf[2]], 
                                    f"{fpath}/{otype2}/{f}" if lvl == '1' else f"{otype1}-{otype2.lower()}/{fpath}", 
                                    otype1, otype2, ftype, lvl, 
                                    db_path, base_path, obs_api_url, test_mode)
                except Exception as e:
                    print("PROCESS_FILE_LS - INVALID conf B", otype1, otype2, ftype, file_ls[:3])
                    print("   - ", e)
                #break
            if test_mode:
                break
    else:
        print("otype_path does not exist", otype_path)

def build_tree(root: str) -> Tree:
    """
    Build a dictionary representing the folder/file hierarchy below `root`.

    Args:
        root: Path to the directory to scan

    Returns:
        Nested dictionary:
        {
            "folderA": {
                "subfolder": {
                    "file.txt": "txt"
                },
                "image.png": "png"
            },
            "file_in_root.py": "py"
        }
    """
    path = Path(root)

    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {root}")
    if not path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {root}")

    tree: Tree = {}

    for item in sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name.lower())):
        if item.is_dir():
            #print("item", item)
            tree[item.name] = build_tree(str(item))
        else:
            tree[item.name] = cast(str, item.name.split(".")[-1])

    return tree

def count_subitems(fpath: str, otype1: str, otype2: str, lvl: str = "2", 
                   db_path: str | Path | None = None, base_path=data_base_path, obs_api_url=None):
    otype_path = f"{base_path}/{fpath}/{otype2}"
    print("otype_path", otype_path, os.path.exists(otype_path))
    if os.path.exists(otype_path):
        dir_subtree: Tree = build_tree(otype_path)
        record_event_baseline(dir_subtree, otype_path, 
                              "baseline subitems", lvl, 
                              meta={'object_type': f'{otype1} / {otype2}'}, 
                              obs_api_url=obs_api_url, db_path=db_path)
    else:
        print("otype_path does not exist", otype_path)



def create_obs_baseline_lvl2(baseline_dict: BaselineConfDict, 
                             lvl: str, base_path: str, 
                             obs_api_url: str | None = None, 
                             test_mode: bool=False) -> None:
    set_run_params()
    run_id = get_run_id()
    script_name = get_script_name()

    os.environ['OBS_RUN_ID'] = str(0)
    os.environ["OBS_SCRIPT_NAME"] = 'obs_baseline_lvl2.py'

    for otype1, def_dict in baseline_dict.items():
        for fpath in def_dict:
            for otype2 in cast(dict, def_dict[fpath]):
                if not cast(dict, def_dict[fpath])[otype2]:
                    continue
                for ftype, (file_ls, meta_conf) in cast(dict, cast(dict, def_dict[fpath])[otype2]).items():
                    print("         ", ftype, file_ls, fpath, otype1, otype2)
                    if file_ls == "do_listdir":
                        process_subdirs(fpath, meta_conf, otype1, otype2, 
                                        ftype, lvl, None, base_path, obs_api_url, test_mode=test_mode)
                    elif file_ls == "do_listdir_gzip":
                        process_subdirs(fpath, meta_conf, otype1, otype2, 
                                        ftype, lvl, None, base_path, obs_api_url, filter_by=".gzip", test_mode=test_mode)
                    elif file_ls == "do_listdir_zip":
                        process_subdirs(fpath, meta_conf, otype1, otype2, 
                                        ftype, lvl, None, base_path, obs_api_url, filter_by=".zip", test_mode=test_mode)
                    elif file_ls == "do_listdir_ways":
                        process_way_dirs(fpath, meta_conf, otype1, otype2, 
                                         ftype, lvl, None, base_path, obs_api_url, test_mode)
                    elif file_ls == "count_subitems":
                        count_subitems(fpath, otype1, otype2, 
                                       lvl, None, base_path, obs_api_url)
                    else:
                        process_file_ls(file_ls, meta_conf, fpath, otype1, otype2, 
                                        ftype, lvl, None, base_path, obs_api_url, test_mode)


    os.environ['OBS_RUN_ID'] = str(run_id)
    os.environ["OBS_SCRIPT_NAME"] = script_name

#obs_api_url = ENV_API_URL if ENV_API_URL else None
#create_obs_baseline_lvl2(BASELINE_DICT['2'], '2', obs_api_url, test_mode=True)


def create_obs_baseline(baseline_dict: BaselineConfDict, lvl: str, 
                        db_path: Path | str, test_mode: bool=False) -> None:
    set_run_params()
    run_id = get_run_id()
    script_name = get_script_name()

    os.environ['OBS_RUN_ID'] = str(0)
    os.environ["OBS_SCRIPT_NAME"] = 'obs_baseline.py'
    
    for otype1, def_dict in baseline_dict.items():
        for fpath in def_dict:
            assert isinstance(def_dict[fpath], dict)
            for otype2 in cast(dict, def_dict[fpath]):
                for ftype, (file_ls, meta_conf) in cast(dict, def_dict[fpath])[otype2].items():
                    print("         ", ftype, file_ls, fpath, otype1, otype2)
                    if file_ls == "do_listdir":
                        process_subdirs(fpath, meta_conf, otype1, otype2, 
                                        ftype, lvl, db_path, test_mode=test_mode)
                    elif file_ls == "do_listdir_ways":
                        process_way_dirs(fpath, meta_conf, otype1, otype2, 
                                         ftype, lvl, db_path, test_mode=test_mode)
                    else:
                        process_file_ls(file_ls, meta_conf, fpath, otype1, otype2, 
                                        ftype, lvl, db_path, test_mode=test_mode)
                #break
            #break
        #break

    os.environ['OBS_RUN_ID'] = str(run_id)
    os.environ["OBS_SCRIPT_NAME"] = script_name


if __name__ == "__main__":
    create_obs_baseline(BASELINE_DICT['1'], '1', db_path, False)