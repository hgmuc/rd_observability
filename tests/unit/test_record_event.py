#import sqlite3
import pandas as pd
import geopandas as gp
import numpy as np
#from pathlib import Path
from shapely.geometry import box, LineString, Point
from rd_observability.observability import record_event
from rd_observability.obs_classes import Result
import rd_observability.observability
import rd_observability.observability_db
import rd_observability.obs_conf

def test_record_event_writes_row(temp_db, monkeypatch, tmp_path):
    # override DB_PATH in observability module too
    monkeypatch.setattr(rd_observability.observability, "DB_PATH", temp_db)
    rd_observability.observability.set_run_params()

    # fake file
    f = tmp_path / "x.pkl"
    f.write_bytes(b"123")

    r = Result(data_cnt=5)
    record_event(r, str(f), "pickle", 
                 meta={"cell":"AA", "object_type":"TEST"}, 
                 db_path = temp_db)

    #conn = sqlite3.connect(temp_db)
    conn = rd_observability.observability.get_connection(temp_db)
    assert conn
    cur = conn.cursor()
    cur.execute("SELECT cell, object_type, rel_count, serializer FROM artifacts")
    row = cur.fetchone()

    assert row == ("AA", "TEST", 5, "pickle")


def test_record_event_writes_row1(temp_db, monkeypatch):
    # override DB_PATH in observability module too
    monkeypatch.setattr(rd_observability.obs_conf, "DB_PATH", temp_db)
    rd_observability.observability.set_run_params()

    # data dict
    d = {1: (1,2,3,4,box(2,3,5,7)), 
         2: (2,3,4,5,Point(4,4)), 
         3: (3,4,None,6,Point(3,3)),
         4: (3,4,None,6,LineString([[3,3], [1,1]])),
         5: (3,4,None,6,LineString())
        }

    record_event(d, "SAMPLE_DICT_WITH_GEO", "gzip_pickle", 
                 meta={"cell":"AA", "object_type":"admin"}, 
                 db_path = rd_observability.obs_conf.DB_PATH)

    #conn = sqlite3.connect(temp_db)
    conn = rd_observability.observability.get_connection(rd_observability.obs_conf.DB_PATH)
    assert conn
    cur = conn.cursor()
    cur.execute("SELECT cell, object_type, rel_count, serializer, polygon_count, multi_polygon_count, line_count, multi_line_count, point_count, empty_count FROM artifacts")
    row = cur.fetchone()

    assert row == ("AA", "admin", 5, "gzip_pickle", 1, 0, 2, 0, 2, 1)


def test_record_event_writes_row2(temp_db, monkeypatch, tmp_path):
    # set run_id / script_name via meta
    monkeypatch.setattr(rd_observability.obs_conf, "DB_PATH", temp_db)
    rd_observability.observability.set_run_params()
    
    # data dict
    d = {1: (1,2,3,4,box(2,3,5,7)), 
         2: (2,3,4,5,Point(4,4)), 
         3: (3,4,None,6,Point(3,3)),
         4: (3,4,None,6,LineString([[3,3], [1,1]])),
         5: (3,4,None,6,LineString())
        }

    record_event(d, 
                 "SAMPLE_DICT_WITH_GEO", "gzip_pickle", 
                 meta={"cell":"AA", "object_type":"admin", 
                       "run_id": 123, "script_name": "test_script.py"}, 
                 db_path = rd_observability.obs_conf.DB_PATH)

    #conn = sqlite3.connect(temp_db)
    conn = rd_observability.observability_db.get_connection(tmp_path / "test_obs.sqlite")
    assert conn
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT cell, object_type, run_id, script_name, rel_count, serializer, polygon_count, multi_polygon_count, line_count, multi_line_count, point_count, empty_count FROM artifacts")
        row = cur.fetchone()

        print("ROW", row)
        assert row == ("AA", "admin", 123, "test_script.py", 5, "gzip_pickle", 1, 0, 2, 0, 2, 1)


def test_record_event_writes_row3(temp_db, monkeypatch):
    # override DB_PATH in observability module too
    monkeypatch.setattr(rd_observability.obs_conf, "DB_PATH", temp_db)
    rd_observability.observability.set_run_params()

    # data dict
    d = {1: (1,2,3,4,box(2,3,5,7)), 
         2: (2,3,4,5,Point(4,4)), 
         3: (3,4,None,6,Point(3,3)),
         4: (3,4,None,6,LineString([[3,3], [1,1]])),
         5: (3,4,None,6,LineString())
        }
    
    df = pd.DataFrame(d).transpose().iloc[:,:4]
    df.columns = ['A', 'B', 'C', 'D']

    record_event(df, "SAMPLE_DF", "df", 
                 meta={"cell":"AA", "dims": 2, "shape": (5, 2), "object_type":"admin"}, 
                 db_path = rd_observability.obs_conf.DB_PATH)

    #conn = sqlite3.connect(temp_db)
    conn = rd_observability.observability.get_connection(rd_observability.obs_conf.DB_PATH)
    assert conn
    cur = conn.cursor()
    cur.execute("SELECT cell, object_type, rel_count, dims, serializer, polygon_count, multi_polygon_count, line_count, multi_line_count, point_count, empty_count FROM artifacts")
    row = cur.fetchone()

    assert row == ("AA", "admin", 5, 2, "df", 0, 0, 0, 0, 0, 0)


def test_record_event_writes_row4(temp_db, monkeypatch):
    # override DB_PATH in observability module too
    monkeypatch.setattr(rd_observability.obs_conf, "DB_PATH", temp_db)
    rd_observability.observability.set_run_params()

    # data dict
    d = {1: (1,2,3,4,box(2,3,5,7)), 
         2: (2,3,4,5,Point(4,4)), 
         3: (3,4,None,6,Point(3,3)),
         4: (3,4,None,6,LineString([[3,3], [1,1]])),
         5: (3,4,None,6,LineString())
        }
    
    df = pd.DataFrame(d).transpose()
    df.columns = ['A', 'B', 'C', 'D', 'geometry']
    gdf = gp.GeoDataFrame(df)

    record_event(gdf, "SAMPLE_GDF_WITH_GEO", "gdf", 
                 meta={"cell":"AA", "dims": 2, "shape": (5, 2), "object_type":"admin"}, 
                 db_path = rd_observability.obs_conf.DB_PATH)

    #conn = sqlite3.connect(temp_db)
    conn = rd_observability.observability.get_connection(rd_observability.obs_conf.DB_PATH)
    assert conn
    cur = conn.cursor()
    cur.execute("SELECT cell, object_type, rel_count, dims, serializer, polygon_count, multi_polygon_count, line_count, multi_line_count, point_count, empty_count FROM artifacts")
    row = cur.fetchone()

    assert row == ("AA", "admin", 5, 2, "gdf", 1, 0, 2, 0, 2, 1)


def test_record_event_writes_row5(temp_db, monkeypatch):
    # override DB_PATH in observability module too
    monkeypatch.setattr(rd_observability.obs_conf, "DB_PATH", temp_db)
    rd_observability.observability.set_run_params()

    # 4D-array
    arr = np.ones((7,4,5,6))
    
    record_event(arr, "NUMPY 4D Array", "Numpy Array", 
                 meta={"cell":"AA", "dims": len(arr.shape), "shape": arr.shape, "object_type":"cluster"}, 
                 db_path = rd_observability.obs_conf.DB_PATH)

    conn = rd_observability.observability.get_connection(rd_observability.obs_conf.DB_PATH)
    assert conn
    cur = conn.cursor()
    cur.execute("SELECT cell, object_type, rel_count, dims, serializer, polygon_count, multi_polygon_count, line_count, multi_line_count, point_count, empty_count FROM artifacts")
    row = cur.fetchone()

    assert row == ("AA", "cluster", 7, 4, "Numpy Array", 0, 0, 0, 0, 0, 0)

