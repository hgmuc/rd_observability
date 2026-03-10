from __future__ import annotations
import os
import pandas as pd
import geopandas as gp
from shapely.geometry.base import BaseGeometry
from typing import cast

from basic_helpers.config_reg_keys import REG_KEYS, RegKeys
from basic_helpers.file_helper import get_c01_from_cell, do_zip_extract

from basic_helpers.config_pbf import FnamesValid
from basic_helpers.types_osm import CellKey

from shape2code.code2shape import codestr_to_shape, fix_mline


def get_key(fname: str) -> FnamesValid:
    return cast(FnamesValid, fname.split(".")[0].split("_")[-1])

def get_cell(fname: str, ftype: str, otype1: str, otype2: str) -> CellKey:
    print("get_cell fname", fname, ftype, otype1, otype2)
    if otype1 == 'ways' and 'Destination' in fname:
        print("get_cell A", fname.split("_")[0])
        return fname.split("_")[0]
    elif otype1 != 'rd-geocode':
        print("get_cell B", fname.replace(otype1, ""), fname.replace(otype1, "").replace(ftype, ""))
        cell = fname.replace(otype1, "").replace(ftype, "")
        if cell[-1] == ".":
            cell = cell[:-1]
        return cell.replace("_", "")
    else:
        # RD-GEOCODE
        print("get_cell C", fname.split("_")[-1].replace(ftype, "")[:-1])
        return fname.split("_")[-1].split(".")[0] #.replace(ftype, "")


def get_reg_key(fpath: str, fname: str, otype: str) -> RegKeys | str:
    otype = otype.replace("_", "-")
    this_fpath = fpath.replace(fname, "").replace(otype, "")
    reg_key = cast(RegKeys, this_fpath.split("/")[-1].split("-")[-1].upper())
    return reg_key if reg_key in REG_KEYS else "--"

def get_object_type(otype1: str, otype2: str, 
                    fpath: str | None = None, lvl: str | None = None) -> str:
    if lvl == '1':
        if otype1 and otype2:
            return f"{otype1} / {otype2}"
        elif otype1:
            return f"{otype1}"
        elif otype2:
            return f"{otype2}"
    else:
        if otype1 and fpath and otype1 != fpath and fpath.upper() not in REG_KEYS:
            if fpath == 'admin-to-cells':
                return f"{otype1} / {otype2}".lower()
            if fpath[:5] == 'admin':
                fpath = fpath[:5] + "_gdf"
            return f"{otype1} / {fpath}"
        elif otype1 and otype2:
            return f"{otype1} / {otype2}"
        elif otype1:
            return f"{otype1}"
        elif fpath:
            return f"{fpath}"
        elif otype2:
            return f"{otype2}"
    
    return "--"

def get_geocode_geom_data(cell: CellKey, reg_key: RegKeys = "DACH", 
                          base_path: str = "/kaggle/input") -> gp.GeoDataFrame:
    c0, c1 = get_c01_from_cell(cell)
    #fpath_gzip = f"{base_path}/rd-geocode-{reg_key.lower()}/{c0}/BASE_DATA_DF_{c0}{c1}.gzip"
    fpath_geom_zip = f"{base_path}/rd-geocode-{reg_key.lower()}/{c0}/GEOMETRY_{c0}{c1}.zip"
    #print("exists", os.path.exists(fpath_gzip),  os.path.exists(fpath_geom_zip))
    #if os.path.exists(fpath_gzip) and os.path.exists(fpath_geom_zip):
    #    base_data = do_ungzip_pkl(fpath_gzip)

    extracted_files = do_zip_extract(fpath_geom_zip)
    
    geom_dict: dict[int, BaseGeometry] = {}
    with open(extracted_files[0]) as f:
        for line in f:
            try:
                idx_str, codestr = line.rstrip().replace("\n", "").replace("\r", "").split("|#|")
                idx = int(idx_str)
                g = codestr_to_shape(codestr)
                if g:
                    geom_dict[idx] = g
            except Exception as e1:
                try:
                    if codestr[:5] == 'mline':
                        g = fix_mline(codestr)
                        if g:
                            geom_dict[idx] = g
                except Exception as e2:
                    print("ERROR #1 get_geocode_geom_data - pos 1", line, e1)
                    print("ERROR #2 get_geocode_geom_data - pos 2", line, e2)

    geom_gdf = gp.GeoDataFrame(pd.DataFrame({'geometry': list(geom_dict.values())}, 
                                            index=list(geom_dict)), crs="EPSG:4326")
    
    # Clean up temporarily extracted file
    if os.path.exists(extracted_files[0]):
        os.remove(extracted_files[0])
    
    return geom_gdf


