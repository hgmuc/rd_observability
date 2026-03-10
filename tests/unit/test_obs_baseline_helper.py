from rd_observability.obs_baseline_helper import get_key, get_cell, get_reg_key, get_object_type

import pytest

def test_get_key():
    fname = "HIGHWAYS_DE-BR.gzip"
    assert get_key(fname) == 'DE-BR'

@pytest.mark.parametrize("fname,expected_key", [
    ("HIGHWAYS_DE-BB.gzip", "DE-BB"),
    ("HIGHWAYS_DE-SAAR.gzip", "DE-SAAR"),
    ("COASTLINE_NL-ZE.pkl", "NL-ZE"),
    ("TOURISM_NODES_ES-AN.pkl", "ES-AN"),
    ("TOURISM_NODES_AT.gzip", "AT"),    
    ("RAILWAYS_GR.gzip", "GR"),
    ("FARMLAND_PT.gzip", "PT"),    
])
def test_get_key1(fname, expected_key):
    key = get_key(fname)
    assert expected_key == key

def test_get_cell():
    fname = "EC.gzip"
    ftype = "gzip"
    otype1 = ""
    otype2 = ""
    assert get_cell(fname, ftype, otype1, otype2) == "EC"

@pytest.mark.parametrize("fname,ftype,otype1,otype2,expected_key", [
    ("CM.gzip", "gzip", "", "", "CM"),
    ("CM.gzip", "gzip", "ways", "", "CM"),
    ("CM.gzip", "gzip", "ways", "highways", "CM"),
    ("00.gzip", "gzip", "", "", "00"),
    ("00.gzip", "gzip", "nodes", "tourism_nodes", "00"),
    ("00.gzip", "gzip", "env_ways", "railways", "00"),
    ("10.gzip", "gzip", "", "", "10"),
    ("10.gzip", "gzip", "loc2adm", "locality_nodes", "10"),
    ("10.gzip", "gzip", "nodes2adm", "", "10"),
    ("loc2adm_BL.gzip", "gzip", "loc2adm", "locality_nodes", "BL"),
    ("S1.gzip", "gzip", "", "", "S1"),
    ("S1.gzip", "gzip", "test", "test", "S1"),
    ("C_a.gzip", "gzip", "", "", "Ca"),
    ("C_a.gzip", "gzip", "test", "test", "Ca"),
    ("_zM.gzip", "gzip", "", "", "zM"),
    ("_zM.gzip", "gzip", "test", "test", "zM"),
    ("_z_b.gzip", "gzip", "", "", "zb"),
    ("_z_b.gzip", "gzip", "test", "test", "zb"),
])
def test_get_cell1(fname, ftype, otype1, otype2, expected_key):
    cell = get_cell(fname, ftype, otype1, otype2)
    assert expected_key == cell

def test_get_reg_key():
    fpath = "/kaggle/input/admin4-dach"
    fname = "9D.txt"
    otype = "admin"
    assert get_reg_key(fpath, fname, otype) == "DACH"

@pytest.mark.parametrize("fpath,fname,otype,expected_key", [
    ("/kaggle/input/admin4-dach", "AM.txt", "admin", "DACH"),
    ("/kaggle/input/admin4-america", "AM.txt", "admin", "--"),
    ("/kaggle/input/env-mountain-passes-dach", "CM.txt", "env", "DACH"),
    ("/kaggle/input/env-mountain-passes-sud", "_zM.txt", "env", "SUD"),
    ("/kaggle/input/env-region-scan", "OM.txt", "env", "SCAN"),
    ("/kaggle/input/env-region-wnw", "D9.txt", "env", "WNW"),
    ("/kaggle/input/env-region-wnw", "T3.txt", "env", "WNW"),
    ("/kaggle/input/env-region-iberia", "10.txt", "env", "IBERIA"),
    ("/kaggle/input/env-region-sudest", "_y_m.txt", "env", "SUDEST"),
])
def test_get_reg_key1(fpath, fname, otype, expected_key):
    reg_key = get_reg_key(fpath, fname, otype)
    assert expected_key == reg_key

def test_get_object_type():
    otype1 = 'env'
    otype2 = 'park'
    lvl = "1"
    assert get_object_type(otype1, otype2, lvl=lvl) == "env / park"

def test_get_object_type1():
    otype1 = 'add-features'
    otype2 = 'tagset'
    lvl = "2"
    assert get_object_type(otype1, otype2, lvl=lvl) == "add-features / tagset"

@pytest.mark.parametrize("otype1,otype2,expected_key", [
    ("env", "terrain", "env / terrain"),
    ("env", None, "env"),
    (None, "admin", "admin")
])
def test_get_object_type2(otype1, otype2, expected_key):
    object_type = get_object_type(otype1, otype2)
    assert expected_key == object_type

@pytest.mark.parametrize("otype1,otype2,fpath,lvl,expected_key", [
    ("env", "terrain", None, None, "env / terrain"),
    ("env", "terrain", None, "1", "env / terrain"),
    ("env", "terrain", None, "2", "env / terrain"),
    ("env", None, None, None, "env"),
    ("env", None, None, "1", "env"),
    ("env", None, None, "2", "env"),
    (None, "admin", None, None, "admin"),
    (None, "admin", None, "1", "admin"),
    (None, "admin", None, "2", "admin"),
    ("admin", "admin", None, None, "admin / admin"),
    ("admin", "admin", None, "1", "admin / admin"),
    ("admin", "admin", None, "2", "admin / admin"),
    ("admin", "csv", "admin", None, "admin / csv"),
    ("admin", "csv", "admin", "1", "admin / csv"),
    ("admin", "csv", "admin", "2", "admin / csv"),
    ("admin", "ADMIN-TO-CELLS", "admin-to-cells", "2", "admin / admin-to-cells"),
    ("admin", "hierarchy", "admin-to-cells", None, "admin / hierarchy"),
    ("admin", "hierarchy", "admin-to-cells", 1, "admin / hierarchy"),
    ("admin", "hierarchy", "admin-to-cells", "2", "admin / hierarchy"),
    ("admin", "region_bbox", "admin-to-cells", "2", "admin / region_bbox"),
    ("admin", "dach", "admin6", "2", "admin / admin_gdf"),
    ("admin", "scan", "admin10", "2", "admin / admin_gdf"),
    ("admin", "scan", "code2admin", "2", "admin / code2admin"),
])
def test_get_object_type3(otype1, otype2, fpath, lvl, expected_key):
    object_type = get_object_type(otype1, otype2, fpath, lvl)
    assert expected_key == object_type


@pytest.mark.parametrize("otype1,otype2,fpath,lvl,expected_key", [
    ("env", "terrain", None, None, "env / terrain"),
    ("env", "terrain", None, "1", "env / terrain"),
    ("env", "terrain", None, "2", "env / terrain"),
    ("env", None, None, None, "env"),
    ("env", None, None, "1", "env"),
    ("env", None, None, "2", "env"),
    ("env", "dach", "park", None, "env / park"),
    ("env", "scan", "meadows", "1", "env / scan"),
    ("env", "scan", "meadows", "2", "env / meadows"),
    ("env", "wnw", "forest", "2", "env / forest"),
    ("env", "wnw", "locality_nodes", "2", "env / locality_nodes"),
    ("env", "wnw", "panorama", "2", "env / panorama"),
    ("env", "wnw", "highways", "2", "env / highways"),
    ("env", "wnw", "coastline", "2", "env / coastline"),
    ("ways", "guideposts", "wnw", "2", "ways / guideposts"),
    ("ways", "guideposts", "ost", "2", "ways / guideposts"),
    ("ways", "barrier_nodes", "sudest", "2", "ways / barrier_nodes"),
    ("ways", "sudest", "_z", "2", "ways / _z"),
    ("ways", "dach", "C", "2", "ways / C"),
    ("ways", "iberia", "3", "2", "ways / 3"),
    ("ways", "ost", "U", "2", "ways / U"),
    ("ways", "ost", "U", "1", "ways / ost"),
    ("ways", "ost", "U", None, "ways / U"),
    ("rd-geocode", "sudest", "_z", "2", "rd-geocode / _z"),
    ("rd-geocode", "dach", "C", "2", "rd-geocode / C"),
    ("rd-geocode", "iberia", "3", "2", "rd-geocode / 3"),
    ("rd-geocode", "ost", "U", None, "rd-geocode / U"),
    ("rd-geocode", "ost", "U", "1", "rd-geocode / ost"),
    ("rd-geocode", "ost", "U", "2", "rd-geocode / U"),
    ("add-features", "ost", "fast_food", None, "add-features / fast_food"),
    ("add-features", "ost", "fast_food", "1", "add-features / ost"),
    ("add-features", "ost", "fast_food", "2", "add-features / fast_food"),
    ("add-features", "ost", "tagset", None, "add-features / tagset"),
    ("add-features", "ost", "tagset", "1", "add-features / ost"),
    ("add-features", "ost", "tagset", "2", "add-features / tagset"),
])
def test_get_object_type4(otype1, otype2, fpath, lvl, expected_key):
    object_type = get_object_type(otype1, otype2, fpath, lvl)
    assert expected_key == object_type
