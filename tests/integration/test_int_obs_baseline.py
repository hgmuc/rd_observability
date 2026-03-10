# type: ignore
from __future__ import annotations
import os
#import json
import pytest
import rd_observability.obs_baseline as obs_baseline
#import rd_observability.observability as observability
#import pandas as pd
#from shapely.geometry import Polygon, box
from typing import cast

#from rd_observability.observability_db import get_connection
from rd_observability.obs_baseline_conf import BaselineConfDict

#from basic_helpers.file_helper import do_gzip_pkl
#import sqlite3
#sys.path.append("C:/05_Python/osm")
#from file_helper import do_pickle, do_gzip_pkl, do_ungzip_pkl, do_unpickle


@pytest.fixture
def capture_post(monkeypatch):
    calls = []
    def fake_post(url, json, timeout):
        calls.append(json)
    monkeypatch.setattr(obs_baseline.requests, "post", fake_post)
    return calls


@pytest.mark.localdata
@pytest.mark.integration
def test_lvl2_env_types(capture_post, monkeypatch):
    monkeypatch.setattr(obs_baseline, "OBS_MODE", "api")
    os.environ["OBS_RUN_ID"] = "0"
    os.environ["OBS_SCRIPT_NAME"] = "obs_baseline_lvl2.py"

    base_path = "C:/05_Python/kaggle_env_api/tests/test_data"
    otype1 = "admin"
    fpath = "admin6"
    otype2 = "dach"
    lvl = "2"
    obs_api_url = "http://fake"
    test_mode = True
    reg_key = otype2
    meta_conf = [None, None, reg_key]
    ftype = "df"
    obs_baseline.process_subdirs(fpath, meta_conf, otype1, otype2, 
                                 ftype, lvl, None, base_path, obs_api_url, test_mode=test_mode)
    payload = capture_post[0]
    print("PAYLOAD", payload)
    assert payload.get("run_id") == 0 
    assert payload.get("script_name") == "obs_baseline_lvl2.py"
    assert payload["key"] is None
    assert payload["cell"] == "AM"
    assert payload["reg_key"] == "DACH"

    assert payload["lvl"] == '2'
    assert payload["filename"] == 'C:/05_Python/kaggle_env_api/tests/test_data/admin6-dach/AM.txt'
    assert payload["serializer"] == 'baseline txt'
    assert payload["metrics_json"]["rel_cnt"] == 7
    assert "top_level_keys" not in payload["metrics_json"]
    assert "columns" in payload["metrics_json"]
    assert payload["metrics_json"]["rows"] == 7
    assert payload["metrics_json"]["cols"] == 5
    assert payload["metrics_json"]["type"] == "GeoDataFrame"
    assert payload["metrics_json"]["geom_empty_count"] == 0
    assert payload["metrics_json"]["geom_invalid_count"] == 0
    assert payload["metrics_json"]["geom_types"] == {'Polygon': 6, 'MultiPolygon': 1}
    assert payload["metrics_json"].get("file_types") is None
    assert payload["metrics_json"].get("value_type_counts") is None
    assert payload["object_type"] == 'admin / admin_gdf'

@pytest.mark.localdata
def test_lvl2_env_types1(capture_post, monkeypatch):
    monkeypatch.setattr(obs_baseline, "OBS_MODE", "api")
    os.environ["OBS_RUN_ID"] = "0"
    os.environ["OBS_SCRIPT_NAME"] = "obs_baseline_lvl2.py"

    base_path = "C:/05_Python/kaggle_env_api/tests/test_data"
    otype1 = "env"
    fpath = "forest"
    otype2 = "dach"
    lvl = "2"
    obs_api_url = "http://fake"
    test_mode = True
    reg_key = otype2
    meta_conf = [None, None, reg_key]
    ftype = "df"
    obs_baseline.process_subdirs(fpath, meta_conf, otype1, otype2, 
                                 ftype, lvl, None, base_path, obs_api_url, test_mode=test_mode)
    payload = capture_post[0]
    print("PAYLOAD", payload)
    assert payload.get("run_id") == 0 
    assert payload.get("script_name") == "obs_baseline_lvl2.py"
    assert payload["key"] is None
    assert payload["cell"] == "AM"
    assert payload["reg_key"] == "DACH"

    assert payload["lvl"] == '2'
    assert payload["filename"] == 'env-forest-dach/AM.txt'
    assert payload["serializer"] == 'baseline txt'
    assert payload["metrics_json"]["rel_cnt"] == 15
    assert "top_level_keys" not in payload["metrics_json"]
    assert "columns" in payload["metrics_json"]
    assert payload["metrics_json"]["rows"] == 15
    assert payload["metrics_json"]["cols"] == 6
    assert payload["metrics_json"]["type"] == "GeoDataFrame"
    assert payload["metrics_json"]["geom_empty_count"] == 0
    assert payload["metrics_json"]["geom_invalid_count"] == 0
    assert payload["metrics_json"]["geom_types"] == {'Polygon': 15}
    assert payload["metrics_json"].get("file_types") is None
    assert payload["metrics_json"].get("value_type_counts") is None
    assert payload["object_type"] == 'env / forest'

@pytest.mark.localdata
def test_lvl2_ways(capture_post, monkeypatch):
    monkeypatch.setattr(obs_baseline, "OBS_MODE", "api")
    os.environ["OBS_RUN_ID"] = "0"
    os.environ["OBS_SCRIPT_NAME"] = "obs_baseline_lvl2.py"

    base_path = "C:/05_Python/kaggle_env_api/tests/test_data"
    otype1 = "ways"
    otype2 = "A"
    fpath = "dach"
    lvl = "2"
    obs_api_url = "http://fake"
    test_mode = True
    reg_key = fpath
    meta_conf = [None, None, reg_key]
    ftype = "df"
    obs_baseline.process_way_dirs(fpath, meta_conf, otype1, otype2, 
                                  ftype, lvl, None, base_path, obs_api_url, test_mode)
    payload = capture_post[0]
    print("PAYLOAD", payload)
    assert payload.get("run_id") == 0 
    assert payload.get("script_name") == "obs_baseline_lvl2.py"
    assert payload["key"] is None
    assert payload["cell"] == "AL"
    assert payload["reg_key"] == "DACH"

    assert payload["lvl"] == '2'
    assert payload["filename"] == 'C:/05_Python/kaggle_env_api/tests/test_data/ways-dach/A/AL.txt'
    assert payload["serializer"] == 'baseline txt'
    assert payload["metrics_json"]["rel_cnt"] == 10
    assert "top_level_keys" not in payload["metrics_json"]
    assert "columns" in payload["metrics_json"]
    assert payload["metrics_json"]["rows"] == 10
    assert payload["metrics_json"]["cols"] == 4
    assert payload["metrics_json"]["type"] == "DataFrame"
    assert payload["metrics_json"].get("geom_types") is None
    assert payload["metrics_json"].get("file_types") is None
    assert payload["metrics_json"].get("value_type_counts") is None
    assert payload["object_type"] == 'ways / A'  

@pytest.mark.slow
@pytest.mark.localdata
def test_lvl2_env_types2(capture_post, monkeypatch):
    monkeypatch.setattr(obs_baseline, "OBS_MODE", "api")
    os.environ["OBS_RUN_ID"] = "0"
    os.environ["OBS_SCRIPT_NAME"] = "obs_baseline_lvl2.py"
    
    #bl_dict = {}
    #bl_dict = {'env': {'waterways': {k: v for k, v in obs_baseline.BASELINE_DICT['2']['env']['waterways'].items() if k == 'dach'}}}
    bl_dict: BaselineConfDict = {'env': {k: {k2: v2 for k2, v2 in v.items() if k2 == 'dach'} 
                       for k, v in obs_baseline.BASELINE_DICT['2']['env'].items() 
                       if v and k in [
                           'farmland', 'waypoints', 'forest', 'locality-nodes', 'natural-peaks', 
                           'mountain-passes', 'region', 'coastline', 'tourism-nodes', 'terrain', 
                           'highways', 'railways', 'waterways', 'treerows', 'tourism-areas']}}
    
    bl_dict['admin'] = {adm_key: {k: v for k, v in cast(dict, obs_baseline.BASELINE_DICT['2']['admin'][adm_key]).items() 
                                  if k == 'dach'} 
                        for adm_key in ['admin4', 'admin5', 'admin6', 'admin7', 'admin8', 'admin10']}
    bl_dict['admin']['code2admin'] = {k: v for k, v in cast(dict, obs_baseline.BASELINE_DICT['2']['admin']['code2admin']).items() if k == 'dach'}
    bl_dict['admin']['admin-to-cells'] = {k: v for k, v in cast(dict, obs_baseline.BASELINE_DICT['2']['admin']['admin-to-cells']).items()}
    bl_dict['admin']['admin'] = {k: v for k, v in cast(dict, obs_baseline.BASELINE_DICT['2']['admin']['admin']).items() if k == 'dach'}
    # ['code2admin', 'admin', 'admin-to-cells']
    bl_dict['rd-geocode'] = {k: {k2: v2 for k2, v2 in v.items() if k2 == 'A'} 
                             for k, v in obs_baseline.BASELINE_DICT['2']['rd-geocode'].items() 
                             if v and k == 'dach'}
    bl_dict['add-features'] = {k: {k2: v2 for k2, v2 in v.items()} 
                               for k, v in obs_baseline.BASELINE_DICT['2']['add-features'].items() 
                               if v and k == 'dach'}
    bl_dict['ways'] = {k: {k2: v2 for k2, v2 in v.items()}  # if k2 == 'A'
                               for k, v in obs_baseline.BASELINE_DICT['2']['ways'].items() if v}
    print("bl_dict", list(bl_dict['admin']))
    obs_baseline.create_obs_baseline_lvl2(bl_dict, "2", 
                                          "C:/05_Python/kaggle_env_api/tests/test_data", 
                                          obs_api_url="http://fake", test_mode=False)
    assert len(capture_post) > 0
    print("capture_post", len(capture_post))
    for cp in capture_post:
        print("capture_post", cp)
    #assert 0 == 1
