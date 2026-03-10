from __future__ import annotations
from typing import TypeAlias, Callable, cast

from basic_helpers.config_reg_keys import REG_KEYS

# --- Metadata Layer (The [None, None, reg_key] part) ---
# It can be a String, a Function, or None.
MetadataItem: TypeAlias = str | Callable[..., str] | None

# This represents the list of 3 items, e.g., [get_key, get_cell, None]
# Using a list of a Union is most flexible for your list comprehensions.
MetadataTriplet: TypeAlias = list[MetadataItem]

# --- File/Command Layer ---
# This is the "do_listdir" string or the list of filenames
FileOrCommand: TypeAlias = str | list[str]

# 2. A list of None values (likely placeholders for metadata) or a string or a function
FileEntry: TypeAlias = list[FileOrCommand | MetadataTriplet]

# The middle layer: e.g., {'csv': {'df': ...}}
# It maps a format (csv, pkl, gzip) to a FileEntry or None
FormatMapping: TypeAlias = dict[str, FileEntry | None]

# The top level structure
BaselineConfDict: TypeAlias = dict[str, dict[str, FormatMapping | None]]

#BaselineConfDict = dict[str, dict[str, 
#                                  #dict[str, list[list[object]]] | 
#                                  dict[str, list[list[Any]]] | 
#                                  dict[str, Sequence[str | list[Any]]] | 
#                                  None]]


ADMIN_BASELINE_DICT: BaselineConfDict = {'admin': 
                        {'csv': {'df': [['admin10.csv', 'admin2.csv', 'admin4.csv', 'admin5.csv', 
                                          'admin6.csv', 'admin65.csv', 'admin7.csv', 'admin8.csv',
                                          'admin87.csv', 'admin9.csv'], cast(MetadataTriplet, [None, None, None])]
                                   }, 
                         'hierarchy': {'pkl': [['ADMIN_HIERARCHY.pkl', 'ADMIN_ID_HIERARCHY.pkl'], 
                                               cast(MetadataTriplet, [None, None, None])]},
                         'hierarchy / xtra': {'pkl': [['ADMIN_HIERARCHY_LVL5.pkl', 'ADMIN_ID_HIERARCHY_LVL5.pkl', 
                                                       'ADMIN_HIERARCHY_LVL7.pkl', 'ADMIN_ID_HIERARCHY_LVL7.pkl'], 
                                                       cast(MetadataTriplet, [None, None, None])]},
                        }, 
                      'lvl1': 
                        {'partial': {'gzip': [["PARTIAL_DICT.gzip", "PARTIAL_DICT_WAYS.gzip"], 
                                              cast(MetadataTriplet, [None, None, None])]},
                         'admin_dict': {'gzip': [[f"ADMIN_DICT_{n}.gzip" for n in [2,4,5,6,7,8,9,10,11]], 
                                                 cast(MetadataTriplet, [None, None, None])]},
                         'code2admin': {'pkl': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
                         'admin / export': None,
                        },
                        }

LOC2ADMIN_BL_DICT: BaselineConfDict = {'admin': {'admin2cells': {'gzip': [["ADMIN_TO_CELLS.gzip"], cast(MetadataTriplet, [None, None, None])]}},
                     'lvl1':  {'locality_nodes': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
                               'tourism_nodes': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
                               'waypoints': {'pkl': ["do_listdir", cast(MetadataTriplet, [None, None, None])], 
                                             'df': [['_WAYPOINTS/waypoints_new.csv'], cast(MetadataTriplet, [None, None, None])], 
                                             'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
                              }}

NODES2ADMIN_BL_DICT: BaselineConfDict = {'lvl1': {'natural_peaks': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
                                'mountain_passes': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
                                'guideposts': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
                                }}

#NODESBRKDWN_BL_DICT = {'lvl1': {'natural_peaks': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
#                                'mountain_passes': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
#                                'guideposts': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
#                                }}

ENV_TYPES = ['farmland', 'forest', 'geology', 'industrial', 'meadows', 'nature_reserve', 
             'park', 'plantation', 'residential', 'residential_attr_dict', 'tourism_areas', 
             'water', 'wetland', 'waypoints']
ENVS_BL_DICT: BaselineConfDict = {'lvl1': {env_type: {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]} for env_type in ENV_TYPES}}
ENVS_BL_DICT['lvl1']['coastline'] = {'pkl': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}

ENV_WAYS_BL_DICT = {'lvl1': {'highways': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]},
                             'railways': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]},
                             'waterways': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]},
                             'treerows': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]},
                             'coastline': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])],
                                           'pkl': ["do_listdir", cast(MetadataTriplet, [None, None, None])]},
                             'region': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])],
                                        'pkl': ["do_listdir", cast(MetadataTriplet, [None, None, None])]},
                             'code2region': {'pkl': [["code2region/CODE2REGION.pkl"], cast(MetadataTriplet, [None, None, None])]}
                             }
                    }

NODES_BL_DICT: BaselineConfDict = {'lvl1': {'locality_nodes': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}, 
                          'tourism_nodes': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]},
                          'barrier': {'gzip': [["BARRIER_NODES.gzip", "BARRIER_DICT.gzip"], cast(MetadataTriplet, [None, None, None])]},
                          'guideposts': {'gzip': [["GUIDEPOST_NODES.gzip"], cast(MetadataTriplet, [None, None, None])]},
                          'mtn passes': {'gzip': [["MOUNTAIN_PASSES.gzip"], cast(MetadataTriplet, [None, None, None])]},
                          'peaks': {'gzip': [["NATURAL_PEAKS.gzip"], cast(MetadataTriplet, [None, None, None])]},
                          'nodes_tagset': {'gzip': [["NODES_TAGSETS_DICT.gzip"], cast(MetadataTriplet, [None, None, None])]},
                          }
                 }


BARRIER_BL_DICT: BaselineConfDict = {'lvl1': {'barrier_nodes': {'gzip': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}}}


WAYS_BL_DICT: BaselineConfDict = {'lvl1': {c if c not in 'yz' else f'_{c}': {'gzip': ["do_listdir_ways", cast(MetadataTriplet, [None, None, None])]} 
                         for c in '0123456789ABCDEFGHIJKLMNOPQRSTUVWyz'}}
WAYS_BL_DICT['lvl1']['lookup'] = {'gzip': [["WAY_ID_LOOKUP.gzip", "CTRY_WAY_LOOKUP.gzip"], cast(MetadataTriplet, [None, None, None])]}
WAYS_BL_DICT['lvl1']['tag_combi'] = {'gzip': [["TAG_COMBI_DICT.gzip", "REV_TAGSET_COMBI_DICT.gzip"], cast(MetadataTriplet, [None, None, None])]}
WAYS_BL_DICT['lvl1']['tagset'] = {'gzip': [["TAGSET_DICT.gzip"], cast(MetadataTriplet, [None, None, None])]}
WAYS_BL_DICT['lvl1']['cw_tagset'] = {'gzip': [["CW_TAGSET_DICT.gzip"], cast(MetadataTriplet, [None, None, None])]}
WAYS_BL_DICT['lvl1']['hike_tagset'] = {'gzip': [["HIKE_TAGSET_DICT.gzip", "REV_HIKE_TAGSET_DICT.gzip"], cast(MetadataTriplet, [None, None, None])]}

WAYS_BL_DICT['lvl1']['surface'] = {'gzip': [["SURFACE_DICT.gzip"], cast(MetadataTriplet, [None, None, None])]}
WAYS_BL_DICT['lvl1']['hike_surface'] = {'gzip': [["HIKING_SURFACE_DICT.gzip"], cast(MetadataTriplet, [None, None, None])]}
WAYS_BL_DICT['lvl1']['missing_hgwy'] = {'gzip': [["MSSNG_HGWY_DICT.gzip"], cast(MetadataTriplet, [None, None, None])]}

WAYS_BL_DICT['lvl1']['nodes_tagset'] = {'gzip': [["NODES_TAGSETS_DICT.gzip"], cast(MetadataTriplet, [None, None, None])]}

WAYS_BL_DICT['lvl1']['region'] = {'gzip': [["REGION_DATA.gzip"], cast(MetadataTriplet, [None, None, None])]}

RELATS_BL_DICT: BaselineConfDict = {'lvl1': {'waterway': {'gzip': [["WATERWAY_REL_WAYS.gzip"], cast(MetadataTriplet, [None, None, None])]}, 
                           'road': {'gzip': [["ROAD_REL_WAYS.gzip"], cast(MetadataTriplet, [None, None, None])]},
                           'railway': {'gzip': [["RAILWAY_REL_WAYS.gzip"], cast(MetadataTriplet, [None, None, None])]}, 
                           'region': {'gzip': [["REGION_DATA.gzip"], cast(MetadataTriplet, [None, None, None])]}, 
                           'coastal_water': {'gzip': [["COASTAL_WATER_DATA.gzip"], cast(MetadataTriplet, [None, None, None])]}, 
                           'superroute': {'pkl': [["SUPERROUTES_DICT.pkl", "SR_ROUTES_DICT.pkl"], cast(MetadataTriplet, [None, None, None])]}, 
                           'route': {'pkl': [["ROUTES_DICT.pkl", "ROUTES_NODES_DICT.pkl", "ROUTES_WAYS_DICT.pkl"], 
                                             cast(MetadataTriplet, [None, None, None])]}, 
                           'hiking_superroute': {'pkl': [["HIKING_SUPERROUTES_DICT.pkl", "HIKING_SR_ROUTES_DICT.pkl"], 
                                                         cast(MetadataTriplet, [None, None, None])]}, 
                           'hiking_route': {'pkl': [["HIKING_ROUTES_DICT.pkl", "HIKING_ROUTES_NODES_DICT.pkl", 
                                                     "HIKING_ROUTES_WAYS_DICT.pkl"], cast(MetadataTriplet, [None, None, None])]}, 
                           'mtb_superroute': {'pkl': [["MTB_SUPERROUTES_DICT.pkl", "MTB_SR_ROUTES_DICT.pkl"], cast(MetadataTriplet, [None, None, None])]}, 
                           'mtb_route': {'pkl': [["MTB_ROUTES_DICT.pkl", "MTB_ROUTES_NODES_DICT.pkl", "MTB_ROUTES_WAYS_DICT.pkl"], cast(MetadataTriplet, [None, None, None])]}, 
                           }, 
                  }

                  
# Level 2 - Cloud Datasets & Notebooks

LVL2_GEOCODE_BL_DICT: BaselineConfDict = {reg_key.lower(): {f"_{c}" if c in 'yz' else f"{c}": {'gzip': ["do_listdir_gzip", cast(MetadataTriplet, [None, None, reg_key])], 
                                                                             'zip / txt': ["do_listdir_zip", cast(MetadataTriplet, [None, None, reg_key])]} 
                                          for c in REG_KEYS[reg_key]['rows'] + REG_KEYS[reg_key].get('xtra_rows', "")} for reg_key in REG_KEYS}

FEAT_TYPES = ['fast_food', 'vending_machines', 'shops', 'lighthouses']
LVL2_ADD_FEAT_BL_DICT: BaselineConfDict = {reg_key.lower(): {feat_type: {'df': ["do_listdir", cast(MetadataTriplet, [None, None, reg_key])]} 
                                                        for feat_type in FEAT_TYPES} for reg_key in REG_KEYS}
for reg_key in REG_KEYS:
    LVL2_ADD_FEAT_BL_DICT[reg_key.lower()]['tagset'] = {'gzip': [["ADD_FEAT_TAGSET_DICT.gzip"], cast(MetadataTriplet, [None, None, reg_key])]}
    # IGNORING - PROCESSED_CHUNKS.gzip (just internal processing status)

LVL2_OPENDATA_BL_DICT: BaselineConfDict = {"bay-dataupdate": {'trails': {'df': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}},
                         "dtz-dataupdate": {'trails': {'df': ["do_listdir", cast(MetadataTriplet, [None, None, None])]}},
                         "bay-imageanalysis": {'images': {'pkl': [["PROC_IMG_DATA.pkl"], cast(MetadataTriplet, [None, None, None])], 
                                                          'items': ["count_subitems", cast(MetadataTriplet, [None, None, None])]}},
                         "dtz-imageanalysis": {'images': {'pkl': [["PROC_IMG_DATA.pkl"], cast(MetadataTriplet, [None, None, None])], 
                                                          'items': ["count_subitems", cast(MetadataTriplet, [None, None, None])]}},
                         "bay-imageexport": {'images': {'pkl': [["PKG_PROCESSING_LOG.pkl"], cast(MetadataTriplet, [None, None, None])],
                                             'items': ["count_subitems", cast(MetadataTriplet, [None, None, None])]}},
                         "dtz-imageexport": {'images': {'pkl': [["PKG_PROCESSING_LOG.pkl"], cast(MetadataTriplet, [None, None, None])],
                                             'items': ["count_subitems", cast(MetadataTriplet, [None, None, None])]}},
                         "bay-gemini-tourtype": {'tourtype': {'pkl': [['tour_types.pkl'], cast(MetadataTriplet, [None, None, None])]}},
                         "bay-gemini-tourtype-v2": {'tourtype': {'pkl': [['tour_classifications.pkl'], cast(MetadataTriplet, [None, None, None])], 
                                                    'df': [['tour_classifications.csv'], cast(MetadataTriplet, [None, None, None])]}},
                         "dtz-gemini-tourtype": {'tourtype': {'pkl': [['tour_types.pkl'], cast(MetadataTriplet, [None, None, None])]}},
                         "dtz-gemini-tourtype-v2": {'tourtype': {'pkl': [['tour_classifications.pkl'], cast(MetadataTriplet, [None, None, None])], 
                                                    'df': [['tour_classifications.csv'], cast(MetadataTriplet, [None, None, None])]}},
                         "gemini-tourtype-review-v2": {'tourtype': {'pkl': [['BAY/review_results.pkl', 
                                                                'DZT/review_results.pkl'], cast(MetadataTriplet, [None, None, None])]}},
                        }



LVL2_ADMIN_BL_DICT: BaselineConfDict = {f"admin{n}": {reg_key.lower(): {'df': ["do_listdir", cast(MetadataTriplet, [None, None, reg_key])]} for reg_key in REG_KEYS} for n in [2,4,5,6,7,8,9,10]}
LVL2_ADMIN_BL_DICT['admin'] = {'csv': {'df': [['admin10.csv', 'admin2.csv', 'admin4.csv', 'admin5.csv', 'admin6.csv', 'admin65.csv', 
                                               'admin7.csv', 'admin8.csv', 'admin87.csv', 'admin9.csv'], cast(MetadataTriplet, [None, None, None])]
                                      }
                               }
LVL2_ADMIN_BL_DICT['code2admin'] = {reg_key.lower(): {'pkl': ["do_listdir", cast(MetadataTriplet, [None, None, reg_key])]} for reg_key in REG_KEYS}
LVL2_ADMIN_BL_DICT['admin-to-cells'] = {'ADMIN_TO_CELLS': {'gzip': [["ADMIN_TO_CELLS.gzip"], cast(MetadataTriplet, [None, None, None])]}, 
                                        'hierarchy': {'pkl': [["ADMIN_ID_HIERARCHY.pkl"], cast(MetadataTriplet, [None, None, None])]},
                                        'region_bbox': {'pkl': [["REGION_BBOX.pkl", "REGION_BBOX_DICT.pkl"], cast(MetadataTriplet, [None, None, None])]}
                                        }

ENV_TYPES_LVL2 = ENV_TYPES.copy() 
ENV_TYPES_LVL2 += ['locality-nodes', 'tourism-nodes', 'natural-peaks', 'mountain-passes', 'terrain', 'panorama', 'tourism-areas'] 
ENV_TYPES_LVL2 += ['highways', 'railways', 'waterways', 'treerows', 'coastline', 'region']

LVL2_ENV_BL_DICT: BaselineConfDict = {env_type: {reg_key.lower(): {'df': ["do_listdir", cast(MetadataTriplet, [None, None, reg_key])]} for reg_key in REG_KEYS} for env_type in ENV_TYPES_LVL2}

LVL2_WAYS_BL_DICT = {reg_key.lower(): {f"_{c}" if c in 'yz' else c: {'df': ["do_listdir_ways", cast(MetadataTriplet, [None, None, reg_key])]} for c in REG_KEYS[reg_key]['rows']} for reg_key in REG_KEYS}
LVL2_WAYS_BL_DICT['guideposts'] = {reg_key.lower(): {'df': ["do_listdir", cast(MetadataTriplet, [None, None, reg_key])]} for reg_key in REG_KEYS}
LVL2_WAYS_BL_DICT['barrier-nodes'] = {reg_key.lower(): {'df': ["do_listdir", cast(MetadataTriplet, [None, None, reg_key])]} for reg_key in REG_KEYS}


# BASELINE_DICT

BASELINE_DICT: dict[str, BaselineConfDict | dict] = {'1': {}, '2': {}, 'opendata': {}}

BASELINE_DICT['1'] = {'admin': ADMIN_BASELINE_DICT, 
                      'loc2adm': LOC2ADMIN_BL_DICT, 
                      'node2adm': NODES2ADMIN_BL_DICT, 
                      #'node_brkdwn': NODESBRKDWN_BL_DICT,  # im Ergebnis redundant mit node2adm, aber falscher
                      'ways': WAYS_BL_DICT,
                      'envs': ENVS_BL_DICT, 
                      'env_ways': ENV_WAYS_BL_DICT,
                      'barrier': BARRIER_BL_DICT,
                      'relats': RELATS_BL_DICT, 
                      'nodes': NODES_BL_DICT,
                    }

BASELINE_DICT['2'] = {'admin': LVL2_ADMIN_BL_DICT, 
                      'env': LVL2_ENV_BL_DICT,
                      'ways': LVL2_WAYS_BL_DICT,
                      'rd-geocode': LVL2_GEOCODE_BL_DICT,
                      # Photon JSONL Downloads - könnten ergänzt werden, 
                      # record_event Calls müssten aber in Kaggle Notebook Code gemacht werden
                      'add-features': LVL2_ADD_FEAT_BL_DICT, 
                      }

BASELINE_DICT['opendata'] = {'opendata': LVL2_OPENDATA_BL_DICT}



# Info BACKUP 
# Notebooks Lvl2: 
# - RD_GEOCODE_<REG_KEY>
# - ENV Terrain <REG_KEY>
# - Add Features <REG_KEY>
# - BAY DataUpdate   => /kaggle/input/bay-dataupdate
# - BAY ImageAnalysis
# - BAY ImageExport
# - BAY_Gemini_TourType_v2
# - Gemini_TourType_Review_v2 (incomplete - 70+ pending review)
# - DTZ DataUpdate
# - DTZ ImageAnalysis
# - DTZ ImageExport
# - DTZ_Gemini_TourType
# - DTZ_Gemini_TourType_v2

# Notebooks Lvl2 - not included in BASELINE CONF yet: 
# - Elev_GEDTM30_COG
# - CONSOLIDATE SUPER DEM
# - ElevTiles_SplitSonny
# - Cluster Analysis <REG_KEY> (NEW)
# - Photon JSONL DOWNLOAD
# - DTZ_BAY_GraphRouteProfileComparison (work in progress)

# Datasets Lvl2
# - ADMIN2 <REG_KEY>
# - ADMIN4 <REG_KEY>
# - ADMIN5 <REG_KEY>
# - ADMIN6 <REG_KEY>
# - ADMIN7 <REG_KEY>
# - ADMIN8 <REG_KEY>
# - ADMIN9 <REG_KEY>
# - ADMIN10 <REG_KEY>
# - ADMIN CSV
# - ADMIN_TO_CELLS  => /kaggle/input/admin-to-cells
# - CODE2ADMIN <REG_KEY>
# - ENV RESIDENTIAL <REG_KEY>
# - ENV FARMLAND <REG_KEY>
# - ENV WETLAND <REG_KEY>
# - ENV WATER <REG_KEY>
# - ENV PARK <REG_KEY>
# - ENV NATURE_RESERVE <REG_KEY>
# - ENV LOCALITY_NODES <REG_KEY>
# - ENV INDUSTRIAL <REG_KEY>
# - ENV FOREST <REG_KEY>
# - ENV MEADOWS <REG_KEY>
# - ENV HIGHWAYS <REG_KEY>
# - ENV GEOLOGY <REG_KEY>
# - ENV PLANTATION <REG_KEY>
# - ENV RAILWAYS <REG_KEY>
# - ENV TOURISM_NODES <REG_KEY>
# - ENV NATURAL_PEAKS <REG_KEY>
# - ENV MOUNTAIN_PASSES <REG_KEY>
# - ENV TOURISM_AREAS <REG_KEY>
# - ENV WATERWAYS <REG_KEY>
# - ENV TREEROWS <REG_KEY>
# - ENV COASTLINE <REG_KEY>
# - ENV REGION <REG_KEY>
# - WAYS GUIDEPOSTS <REG_KEY>
# - WAYS BARRIER_NODES <REG_KEY>
# - WAYS <REG_KEY>

# Some FAILED notbooks: FAILED-DTZ_Gemma9B_Demo, FAILED-DTZ_MistralSmall24B_Demo


