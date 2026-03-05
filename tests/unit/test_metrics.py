import pandas as pd
import geopandas as gp
import numpy as np
from shapely.geometry import box, Point, LineString
from typing import cast

from rd_observability.observability import dict_metrics, df_metrics, gdf_metrics, np_metrics

from basic_helpers.file_helper import do_unpickle
from basic_helpers.config import data_base_path
#sys.path.append("C:/05_Python/osm")
#from file_helper import do_unpickle
#from config_helper import data_base_path

def test_dict_metrics():
    d = {"a": {"b": 1}, "c": 2}
    m = dict_metrics(d)
    print("Max recursive", m)
    assert m["rel_cnt"] == 2
    assert m["top_level_keys"] == 2
    assert cast(int, m["max_depth"]) >= 1
    assert m["type"] == 'dict'

def test_dict_metrics1():
    d = {"a": {"b1": 1, "b2": 2, "v3": 3}, "c": 2}
    m = dict_metrics(d)
    print("Max recursive", m)
    assert m["rel_cnt"] == 4
    assert m["top_level_keys"] == 2
    assert m["total_keys_recursive"] == 5
    assert cast(int, m["max_depth"]) >= 1
    assert m["type"] == 'dict'
    assert cast(dict, m["value_type_counts"])["dict"] == 1
    assert cast(dict, m["value_type_counts"])["int"] == 4
    assert len(cast(dict, m["geom_types"])) == 0
    assert len(cast(dict, m["file_types"])) == 0

def test_dict_metrics2():
    d = {1: (1,2,3,4,None), 2:(2,3,4,5,None), 3: (3,4,None,6,7)}
    m = dict_metrics(d)
    print("Max recursive", m)
    assert m["rel_cnt"] == 3
    assert m["top_level_keys"] == 3
    assert m["total_keys_recursive"] == 3
    assert m["max_depth"] == 1
    assert m["type"] == 'dict'
    assert cast(dict, m["value_type_counts"])["dict"] == 0
    assert cast(dict, m["value_type_counts"])["tuple"] == 3
    assert len(cast(dict, m["geom_types"])) == 0
    assert len(cast(dict, m["file_types"])) == 0

def test_dict_metrics3():
    d = {1: (1,2,3,4,box(2,3,5,7)), 
         2: (2,3,4,5,Point(4,4)), 
         3: (3,4,None,6,Point(3,3)),
         4: (3,4,None,6,LineString([[3,3], [1,1]])),
         5: (3,4,None,6,LineString())
         }
    m = dict_metrics(d)
    print("Max recursive", m)
    assert m["rel_cnt"] == 5
    assert m["top_level_keys"] == 5
    assert m["total_keys_recursive"] == 5
    assert m["max_depth"] == 1
    assert m["type"] == 'dict'
    assert cast(dict, m["value_type_counts"])["dict"] == 0
    assert cast(dict, m["value_type_counts"])["tuple"] == 5
    assert m["geom_empty_count"] == 1
    assert m["geom_invalid_count"] == 0
    assert cast(dict, m["geom_types"])["Polygon"] == 1
    assert cast(dict, m["geom_types"])["Point"] == 2
    assert cast(dict, m["geom_types"])["LineString"] == 2
    assert len(cast(dict, m["geom_types"])) == 3
    assert len(cast(dict, m["file_types"])) == 0

def test_dict_metrics4():
    d = {'CM': {'1': {'track001.gpx': 'gpx', 'track002.gpx': 'gpx', 'track003.gpx': 'gpx'}, 
                '2': {'track011.gpx': 'gpx', 'track012.gpx': 'gpx', 'track013.gpx': 'gpx', 'track014.gpx': 'gpx'}},
         'CN': {'1': {'track001.gpx': 'gpx', 'track002.gpx': 'gpx', 'track003.gpx': 'gpx'}, 
                '3': {'track011.gpx': 'gpx', 'track012.gpx': 'gpx', 'track013.gpx': 'gpx', 'track014.gpx': 'gpx'}}                
         }
    m = dict_metrics(d)
    print("Max recursive", m)
    assert m["rel_cnt"] == 14
    assert m["top_level_keys"] == 2
    assert m["total_keys_recursive"] == 20
    assert m["max_depth"] == 3
    assert m["type"] == 'dict'
    assert cast(dict, m["value_type_counts"])["dict"] == 6
    assert cast(dict, m["value_type_counts"])["str"] == 14
    assert len(cast(dict, m["geom_types"])) == 0
    assert len(cast(dict, m["file_types"])) == 1
    assert cast(dict, m["file_types"])["gpx"] == 14

def test_dict_metrics5():
    d = {'CM': {'1': {'track001.gpx': 'gpx', 'track002.gpx': 'gpx', 'track003.gpx': 'gpx'}, 
                '2': {'track011.gpx': 'gpx', 'track012.gpx': 'gpx', 'track013.gpx': 'gpx', 'track014.gpx': 'gpx'}},
         'CN': {'1': {'track001.md': 'md', 'track002.txt': 'txt', 'track003.html': 'html'}, 
                '3': {'track011.docx': 'docx', 'track012.pptx': 'pptx', 'track013.py': 'py'}}                
         }
    m = dict_metrics(d)
    print("Max recursive", m)
    assert m["rel_cnt"] == 13
    assert m["top_level_keys"] == 2
    assert m["total_keys_recursive"] == 19
    assert m["max_depth"] == 3
    assert m["type"] == 'dict'
    assert cast(dict, m["value_type_counts"])["dict"] == 6
    assert cast(dict, m["value_type_counts"])["str"] == 13
    assert len(cast(dict, m["geom_types"])) == 0
    assert len(cast(dict, m["file_types"])) == 5
    assert cast(dict, m["file_types"])["gpx"] == 7
    assert cast(dict, m["file_types"])["txt"] == 1
    assert cast(dict, m["file_types"])["md"] == 1
    assert cast(dict, m["file_types"])["html"] == 1
    assert cast(dict, m["file_types"])["py"] == 1

def test_dict_metrics6():
    d = do_unpickle(f"{data_base_path}/admin/ADMIN_ID_HIERARCHY.pkl")
    m = dict_metrics(d, max_depth=6)
    print("Max recursive", m)
    assert m["type"] == 'dict'
    assert cast(int, m["rel_cnt"]) > 50_000
    assert cast(int, m["total_keys_recursive"]) > 150_000
    assert cast(int, m["top_level_keys"]) > 50 and cast(int, m["top_level_keys"]) < 60
    assert m["max_depth"] == 5
    assert m["type"] == 'dict'
    assert len(cast(dict, m["geom_types"])) == 0
    assert len(cast(dict, m["file_types"])) == 0
    assert len(cast(dict, m["value_type_counts"])) == 2
    #assert 0 == 1

def test_df_metrics():
    df = pd.DataFrame({"x":[1,2], "y":[3,4]})
    m = df_metrics(df)
    assert m["rel_cnt"] == 2
    assert m["rows"] == 2
    assert m["cols"] == 2
    assert m["type"] == 'DataFrame'

def test_df_metrics1():
    df = pd.DataFrame({"x":[1,2], "y":[3,4]})
    val_x = cast(int, df.loc[0, "x"])
    val_y = cast(int, df.loc[0, "y"])
    df.loc[0, "z"] = val_x / val_y
    df['z2'] = df['x'] / 0
    m = df_metrics(df)
    assert m["rel_cnt"] == 2
    assert m["rows"] == 2
    assert m["cols"] == 4
    assert m["shape"] == (2, 4)
    assert m["dims"] == 2
    assert m["nan_count"] == 1
    assert m["inf_count"] == 2
    assert m["type"] == 'DataFrame'

def test_df_metrics2():
    gdf = pd.DataFrame({"x":[], "y":[], "z": []})
    m = df_metrics(gdf)
    assert m["rel_cnt"] == 0
    assert m["rows"] == 0
    assert m["cols"] == 3
    assert m["columns"] == ['x', 'y', 'z']
    assert m["type"] == 'DataFrame'

def test_df_metrics3():
    gdf = gp.GeoDataFrame(pd.DataFrame({"x":[], "y":[], "geometry": []}))
    m = df_metrics(gdf)
    assert m["rel_cnt"] == 0
    assert m["rows"] == 0
    assert m["cols"] == 3
    assert m["shape"] == (0, 3)
    assert m["dims"] == 2
    assert m["columns"] == ['x', 'y', 'geometry']
    assert m["type"] == 'DataFrame'

def test_gdf_metrics():
    gdf = gp.GeoDataFrame(pd.DataFrame({"x":[], "y":[], "geometry": []}))
    m = gdf_metrics(gdf)
    assert m["rel_cnt"] == 0
    assert m["rows"] == 0
    assert m["cols"] == 3
    assert m["shape"] == (0, 3)
    assert m["dims"] == 2
    assert m["columns"] == ['x', 'y', 'geometry']
    assert m["type"] == 'GeoDataFrame'

def test_np_metrics():
    arr = np.zeros((2,3))
    m = np_metrics(arr)
    assert m["rel_cnt"] == 2
    assert m["shape"] == (2,3)
    assert m["dims"] == 2
    assert m["type"] == 'Numpy Array'

def test_np_metrics1():
    arr = np.zeros((2,3,4))
    m = np_metrics(arr)
    assert m["rel_cnt"] == 2
    assert m["shape"] == (2,3,4)
    assert m["dims"] == 3
    assert m["type"] == 'Numpy Array'

def test_np_metrics_inf():
    arr = np.ones((2,3)) / 0
    m = np_metrics(arr)
    assert m["rel_cnt"] == 2
    assert m["inf_count"] == 6
    assert m["shape"] == (2,3)
    assert m["type"] == 'Numpy Array'

def test_np_metrics_nan():
    df = pd.DataFrame({"x":[1,2], "y":[3,4]})
    df.loc[0, "z"] = cast(int, df.loc[0, "x"]) / cast(int, df.loc[0, "y"])
    m = np_metrics(df.values)
    assert m["rel_cnt"] == 2
    assert m["nan_count"] == 1
    assert m["shape"] == (2,3)
    assert m["type"] == 'Numpy Array'
