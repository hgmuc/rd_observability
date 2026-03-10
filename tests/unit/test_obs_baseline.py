from __future__ import annotations
import os
import json
import pytest
import rd_observability.obs_baseline as obs_baseline
import rd_observability.observability as observability
import pandas as pd
from shapely.geometry import Polygon, box
# from typing import cast

from rd_observability.observability_db import get_connection
#from rd_observability.obs_baseline_conf import BaselineConfDict

from basic_helpers.file_helper import do_gzip_pkl
#import sqlite3
#sys.path.append("C:/05_Python/osm")
#from file_helper import do_pickle, do_gzip_pkl, do_ungzip_pkl, do_unpickle

def test_baseline_pipeline_event(temp_db, monkeypatch):
    monkeypatch.setattr(obs_baseline, "db_path", temp_db)

    data = {"a":{"b":1}}
    #f = tmp_path / "d.pkl"
    #do_pickle(data, f)
    obs_baseline.record_event_baseline(data, "test.pkl", "baseline pkl", lvl='1', 
                                       meta={'object_type': 'testdict', 'lvl': '1'}, 
                                       obs_api_url=None, db_path=temp_db)

    m = {}
    conn = get_connection(temp_db) # sqlite3.connect(temp_db)
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT metrics_json FROM artifacts where run_id = 0")
        m = cur.fetchone()[0]

    assert "total_keys_recursive" in m

def test_baseline_pipeline_event1(temp_db, monkeypatch):
    monkeypatch.setattr(obs_baseline, "db_path", temp_db)

    data = {"a":{"b":1}}
    #f = tmp_path / "d.pkl"
    #do_pickle(data, f)
    obs_baseline.record_event_baseline(data, "test.pkl", "baseline pkl", lvl='1', 
                                       meta={'object_type': 'testdict', 'lvl': '1', 'run_id': 4711, 'script_name': 'GET_BASELINE_SCAN'}, 
                                       obs_api_url=None, db_path=temp_db)

    run_id = script_name = None
    m = {}

    conn = get_connection(temp_db) # sqlite3.connect(temp_db)
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT run_id, script_name, metrics_json FROM artifacts where run_id = 4711")
        run_id, script_name, m = cur.fetchall()[0]
    
    assert "total_keys_recursive" in m
    assert run_id == 4711
    assert script_name == 'GET_BASELINE_SCAN'


def test_baseline_pipeline_event_df(temp_db, monkeypatch):
    monkeypatch.setattr(obs_baseline, "db_path", temp_db)
    df = pd.DataFrame({'a': [1,2,3,4,5], 'b': [0,-2,2,2,-1], 'c': list('GFHHA'), 'x': [1,1,-1,-1, '']})
    #df.to_csv(tmp_path / "df.csv", sep="\t")
    obs_baseline.record_event_baseline(df, "test_df.csv", "baseline df", lvl='1', 
                                       meta={'object_type': 'testdf', 'lvl': '1'}, 
                                       obs_api_url=None, db_path=temp_db)

    m = {}
    conn = get_connection(temp_db) # sqlite3.connect(temp_db)
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT metrics_json FROM artifacts where run_id = 0")
        m = json.loads(cur.fetchone()[0])
    
    print("M", type(m), m)
    assert "columns" in m
    assert "rows" in m
    assert m.get("rows") == 5
    assert m["cols"] == 4
    assert m["columns"] == list('abcx')

# process_file_ls(file_ls, meta_conf, fpath, otype1, otype2, ftype, lvl, db_path
def test_process_file_ls(temp_db, monkeypatch, tmp_path):
    monkeypatch.setattr(obs_baseline, "db_path", temp_db)
    monkeypatch.setattr(obs_baseline, "data_base_path", str(tmp_path))

    # Generate test data and save as file
    data = {"a":{"b":1}}
    f = tmp_path / "d.gzip"
    do_gzip_pkl(data, f)  # file_helper variant !
    #dx = do_ungzip_pkl(f)
    #print("DX", type(dx), dx)

    obs_baseline.process_file_ls(["d.gzip"], [None, None, None], tmp_path, 
                                 "test", "testtest", "gzip", "1", temp_db, 
                                 test_mode=True)
    m = {}
    conn = get_connection(temp_db) # sqlite3.connect(temp_db)
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT metrics_json FROM artifacts")
        m = json.loads(cur.fetchone()[0])
    
    print("M", type(m), m)
    assert "total_keys_recursive" in m

@pytest.fixture
def capture_post(monkeypatch):
    calls = []
    def fake_post(url, json, timeout):
        calls.append(json)
    monkeypatch.setattr(obs_baseline.requests, "post", fake_post)
    return calls

def test_process_file_ls1(temp_db, capture_post, monkeypatch, tmp_path):
    #monkeypatch.setattr(obs_baseline, "db_path", temp_db)
    #monkeypatch.setattr(obs_baseline, "data_base_path", str(tmp_path))
    monkeypatch.setattr(obs_baseline, "OBS_MODE", "api")
    monkeypatch.setattr(observability, "OBS_MODE", "api")

    os.environ["OBS_RUN_ID"] = "1"
    os.environ["OBS_SCRIPT_NAME"] = "obs_baseline_lvl2.py"
    # Generate test data and save as file
    data = {"a":{"b":1}}
    f = tmp_path / "d.gzip"
    do_gzip_pkl(data, f)  # file_helper variant !
    #dx = do_ungzip_pkl(f)
    #print("DX", type(dx), dx)

    obs_baseline.process_file_ls(["d.gzip"], [None, None, None], tmp_path, 
                                 "test", "testtest", "gzip", "1", temp_db, 
                                 obs_api_url="http://fake",
                                 test_mode=True)

    # Ensure API call happened => capture_post IS ALREADY THE LIST of recorded posts
    print("OBS_MODE 1", obs_baseline.OBS_MODE)
    print("OBS_MODE 2", observability.OBS_MODE)
    print("payload", capture_post[0])
    assert len(capture_post) == 1

    payload = capture_post[0]
    assert (str(payload.get("run_id")) == str(observability.get_run_id()) 
            or str(payload.get("run_id"))[:-4] == str(observability.get_run_id())[:-4])
    assert payload.get("script_name") == "obs_baseline_lvl2.py"



def test_process_subdirs(temp_db, monkeypatch, tmp_path):
    monkeypatch.setattr(obs_baseline, "db_path", temp_db)
    monkeypatch.setattr(obs_baseline, "data_base_path", str(tmp_path))
    
    # Generate multiple test data objects and save them as file
    data1 = {-1: {100: [1,2,3,4], 101: [1,2,3,4], 102: [1,2,3,4]}, 
             100: ('N100', None, None, box(1,2,3,4)), 
             101: ('N101', None, None, box(1,2,3,4)), 
             102: ('N102', 'N102-de', None, box(1,2,3,4)), 
             }
    data2 = {-1: {200: [1,2,3,4], 201: [1,2,3,4], 202: [1,2,3,4]}, 
             200: ('N200', None, None, box(1,2,3,4)), 
             201: ('N201', None, None, box(1,2,3,4)), 
             202: ('N202', 'N202-de', None, box(1,2,3,4)), 
             }
    data3 = {-1: {300: [1,2,3,4], 301: [1,2,3,4], 302: [1,2,3,4]}, 
             300: ('N300', None, None, box(1,2,3,4)), 
             301: ('N301', None, None, Polygon()), 
             302: ('N302', 'N302-de', None, box(1,2,3,4)), 
             }
    #f = tmp_path / "d.gzip"
    f1 = tmp_path / "lvl1" / "_FOREST" / "d_DE-BR.gzip"
    f2 = tmp_path / "lvl1" / "_FOREST" / "d_IT-CTRO.gzip"
    f3 = tmp_path / "lvl1" / "_FOREST" / "d_CH.gzip"
    f1.parent.mkdir(parents=True, exist_ok=True)

    do_gzip_pkl(data1, str(f1))  # file_helper variant !
    do_gzip_pkl(data2, str(f2))  # file_helper variant !
    do_gzip_pkl(data3, str(f3))  # file_helper variant !

    obs_baseline.process_subdirs("lvl1", [None, None, None], "envs", "forest", 'gzip', '1', 
                                 temp_db, base_path=obs_baseline.data_base_path, test_mode=False)

    res = [{}] * 3
    conn = get_connection(temp_db) # sqlite3.connect(temp_db)
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT metrics_json FROM artifacts where run_id = 0")
        res = [json.loads(r[0]) for r in cur.fetchall()]
    
    print("res", type(res), len(res), res[-1])
    print("res0", res[0])
    print("res1", res[1])
    print("res2", res[2])

    assert len(res) == 3
    assert len(res[0]) == 10
    assert "geom_empty_count" in res[0]
    assert res[2]["geom_types"]["Polygon"] == 3
    assert res[0]["geom_empty_count"] == 1

    if conn:
        cur = conn.cursor()
        cur.execute("SELECT key, cell, reg_key FROM artifacts where run_id = 0")
        res = cur.fetchall()
        conn.close()

    #res = [r[0]) for r in cur.fetchall()]
    print("res_key", type(res), len(res))
    print("res4", res[0])
    print("res5", res[1])
    print("res6", res[2])

    assert res[0][0] == "CH"
    assert res[1][0] == "DE-BR"
    assert res[2][1] is None


# def process_way_dirs(fpath, meta_conf, otype1, otype2, ftype, lvl, db_path, test_mode
def test_process_way_dirs(temp_db, monkeypatch, tmp_path):
    monkeypatch.setattr(obs_baseline, "db_path", temp_db)
    monkeypatch.setattr(obs_baseline, "data_base_path", str(tmp_path))
    
    # Generate multiple test data objects and save them as file
    data1 = {'00': {'c': {'DE-BR': set([100,101,102,103])}, 
                    'w': {100: (11, 41, 10, 12, None, [[3,3], [4,4], [5,5]]), 
                          101: (11, 51, 11, 12, None, [[3,3], [1,1], [2,2]]), 
                          102: (11, 61, 12, 14, 'Hauptstraße', [[3,3], [0,0], [0,2]]), 
                          103: (51, 81, 9, 11, None, [[8,7], [8,6], [7,4]]), 
                          }, 
                    'n': {}}, 
             '01': {'c': {'DE-BR': set([110,111,112,113])}, 
                    'w': {110: (12, 42, 10, 12, None, [[3,3], [4,4], [5,5]]), 
                          111: (12, 52, 11, 12, None, [[3,3], [1,1], [2,2]]), 
                          112: (12, 62, 12, 14, 'Hauptstraße', [[3,3], [0,0], [0,2]]), 
                          113: (52, 82, 9, 11, None, [[8,7], [8,6], [7,4]]), 
                          }, 
                    'n': {}},                     
             '02': {'c': {'DE-BR': set([120,121,122])}, 
                    'w': {120: (13, 43, 10, 12, None, [[3,3], [4,4], [5,5]]), 
                          121: (13, 53, 11, 12, None, [[3,3], [1,1], [2,2]]), 
                          122: (13, 63, 12, 14, 'Hauptstraße', [[3,3], [0,0], [0,2]]), 
                          }, 
                    'n': {}},                     
            }
    data2 = {'11': {'c': {'DK': set([200,201,202,203])}, 
                    'w': {200: (211, 241, 10, 12, None, [[3,3], [4,4], [5,5]]), 
                          201: (211, 251, 11, 12, None, [[3,3], [1,1], [2,2]]), 
                          202: (211, 261, 12, 14, 'Hauptstraße', [[3,3], [0,0], [0,2]]), 
                          203: (251, 281, 9, 11, None, [[8,7], [8,6], [7,4]]), 
                          }, 
                    'n': {}}, 
             '12': {'c': {'DK': set([210,211]), 'DE-SH': set([212,213])}, 
                    'w': {210: (112, 142, 10, 12, None, [[3,3], [4,4], [5,5]]), 
                          211: (112, 152, 11, 12, None, [[3,3], [1,1], [2,2]]), 
                          212: (112, 162, 12, 14, 'Hauptstraße', [[3,3], [0,0], [0,2]]), 
                          213: (152, 182, 9, 11, None, [[8,7], [8,6], [7,4]]), 
                          }, 
                    'n': {}},                     
             '13': {'c': {'DK': set([220,221,222])}, 
                    'w': {220: (213, 143, 10, 12, None, [[3,3], [4,4], [5,5]]), 
                          221: (213, 153, 11, 12, None, [[3,3], [1,1], [2,2]]), 
                          222: (213, 163, 12, 14, 'Hauptstraße', [[3,3], [0,0], [0,2]]), 
                          }, 
                    'n': {}},                     
            }

    #f = tmp_path / "d.gzip"
    f1 = tmp_path / "lvl1" / "J" / "JH.gzip"
    f2 = tmp_path / "lvl1" / "J" / "JN.gzip"
    f1.parent.mkdir(parents=True, exist_ok=True)

    do_gzip_pkl(data1, str(f1))  # file_helper variant !
    do_gzip_pkl(data2, str(f2))  # file_helper variant !

    obs_baseline.process_way_dirs("lvl1", [None, obs_baseline.get_cell, None], "ways", "J", 'gzip', '1', 
                                  temp_db, base_path=str(tmp_path), test_mode=False)

    conn = get_connection(temp_db) # sqlite3.connect(temp_db)
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT metrics_json FROM artifacts where run_id = 0")
        res = [json.loads(r[0]) for r in cur.fetchall()]
    
    print("res", type(res), len(res), res[-1])
    print("res0", res[0])
    print("res1", res[1])
    #print("data_base_path", obs_baseline.data_base_path)

    assert len(res) == 2
    assert len(res[0]) == 10
    assert "geom_empty_count" in res[0]
    assert res[1]["geom_invalid_count"] == 0
    assert res[0]["geom_empty_count"] == 0
    
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT key, cell, reg_key FROM artifacts where run_id = 0")
        res = cur.fetchall()
        conn.close()
    #res = [r[0]) for r in cur.fetchall()]
    print("res_key", type(res), len(res))
    print("res4", res[0])
    print("res5", res[1])

    assert res[0][1] == "JH"
    assert res[1][1] == "JN"


def test_build_tree():
    doc_tree = obs_baseline.build_tree("./tests/data/files")

    exp_dict = {'CM': {'1': {'track_CM13-690013_CM13-780045.gpx': 'gpx', 
                             'track_CM13-690013_CM13-790097.gpx': 'gpx', 
                             'track_CM13-780045_CM13-690013.gpx': 'gpx', 
                             'track_CM13-780045_CM13-790097.gpx': 'gpx', 
                             'track_CM13-790066_CM13-690013.gpx': 'gpx', 
                             'track_CM13-790097_CM13-690013.gpx': 'gpx', 
                             'track_CM13-790097_CM13-780045_alt.gpx': 'gpx', 
                             'track_CM16-350192_CM16-370236.gpx': 'gpx', 
                             'track_CM16-360190_CM16-350195.gpx': 'gpx', 
                             'track_CM16-370247_CM16-470016.gpx': 'gpx', 
                             'track_CM16-460033_CM16-370245.gpx': 'gpx', 
                             'track_CM16-460086_CM16-370245.gpx': 'gpx', 
                             'track_CM16-470133_CM16-470016.gpx': 'gpx'}, 
                       '2': {'track_CM21-980131_CM21-980183.gpx': 'gpx', 
                             'track_CM23-670050_CM23-770082.gpx': 'gpx'}, 
                       '4': {'track_CM42-740023_CM42-940021.gpx': 'gpx'}}, 
                'CN': {'1': {'track_CN15-490010_CN16-520027.gpx': 'gpx', 'track_CN16-520027_CN15-490010.gpx': 'gpx'}}
                }
    
    assert doc_tree == exp_dict


def test_count_subitems(temp_db):
    os.environ['OBS_RUN_ID'] = str(0)
    os.environ["OBS_SCRIPT_NAME"] = 'obs_baseline_lvl2.py'

    #base_path = "../../"
    #base_path = "C:/Users/helmu/Desktop/Tracks"
    base_path = "./tests"
    fpath = "data"
    otype1 = "opendata"
    otype2 = "files"
    obs_baseline.count_subitems(fpath, otype1, otype2, "2", temp_db, base_path = base_path)
    otype_path = f"{base_path}/{fpath}/{otype2}"
    #assert otype_path == "./tests/test_data/files"
    #assert otype_path == "C:/Users/helmu/Desktop/Tracks/images_dtz/output_zips"
    assert otype_path == "./tests/data/files"
    print("x - otype_path", os.getcwd(), otype_path, os.path.exists(otype_path))
    assert os.path.exists(otype_path)

    m = {}
    object_type = lvl = filename = run_id = script_name = rel_cnt = txt_cnt = gpx_cnt = None
    conn = get_connection(temp_db) # sqlite3.connect(temp_db)
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT object_type, lvl, filename, run_id, script_name, rel_count, txt_count, gpx_count, metrics_json FROM artifacts where run_id == 0")

        object_type, lvl, filename, run_id, script_name, rel_cnt, txt_cnt, gpx_cnt, m = cur.fetchall()[0]
        metrics = json.loads(m)
        conn.close()

    assert "total_keys_recursive" in m
    assert metrics['file_types'] == {'gpx': 18}
    assert metrics['geom_types'] == {}
    assert metrics['max_depth'] == 3
    assert object_type == 'opendata / files'
    assert lvl == '2'
    assert filename == './tests/data/files'
    assert run_id == 0
    assert script_name == 'obs_baseline_lvl2.py'
    assert rel_cnt == 18
    assert txt_cnt == 0
    assert gpx_cnt == 18


def test_count_subitems_api(monkeypatch, capture_post):
    monkeypatch.setattr(obs_baseline, "OBS_MODE", "api")

    os.environ['OBS_RUN_ID'] = str(0)
    os.environ["OBS_SCRIPT_NAME"] = 'obs_baseline_lvl2.py'

    base_path = "./tests"
    fpath = "data"
    otype1 = "opendata"
    otype2 = "files"
    obs_baseline.count_subitems(fpath, otype1, otype2, lvl="2", obs_api_url="http://fake", base_path = base_path)
    otype_path = f"{base_path}/{fpath}/{otype2}"
    assert otype_path == "./tests/data/files"
    assert os.path.exists(otype_path)

    payload = capture_post[0]
    print("PAYLOAD", payload)
    assert payload.get("run_id") == 0 
    assert payload.get("script_name") == "obs_baseline_lvl2.py"
    assert payload["key"] == payload["cell"] == payload["reg_key"] == "--"

    assert payload["object_type"] == 'opendata / files'
    assert payload["lvl"] == '2'
    assert payload["filename"] == './tests/data/files'
    assert payload["serializer"] == 'baseline subitems'
    assert payload["metrics_json"]["rel_cnt"] == 18
    assert payload["metrics_json"]["top_level_keys"] == 2
    assert payload["metrics_json"]["total_keys_recursive"] == 24
    assert payload["metrics_json"]["type"] == "dict"
    assert payload["metrics_json"]["value_type_counts"] == {'dict': 6, 'str': 18}
    assert payload["metrics_json"]["geom_empty_count"] == 0
    assert payload["metrics_json"]["geom_types"] == {}
    assert payload["metrics_json"]["file_types"] == {'gpx': 18}

