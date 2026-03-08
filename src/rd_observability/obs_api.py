from __future__ import annotations
import sqlite3
import json
import os
from pathlib import Path
from typing import Literal
from .observability_db import init_db, get_connection
#from .obs_conf import DB_PATH

# INGEST function / API will be hosted on ENV_API_Server (one per REG_KEY)

def ingest(event: dict, db_path: Path | None = None) -> dict[str, Literal['ok', 'error']]:
    vals = (event.get("timestamp"), event.get("key"), event.get("cell"), 
            event.get("object_type"), event.get("reg_key"), event.get("lvl"),
            event.get("filename"), event.get("serializer"), event.get("size_bytes"),
            event.get("script_name"), event.get("pipeline_version"), event.get("run_id"),
            json.dumps(event.get("metrics_json")), event.get("fingerprint"),
            #event.get("metrics_json"), event.get("fingerprint"),
           )    
    print("METRICS", event.get("metrics_json"), type(event.get("metrics_json")))
    print("RUN ID ", event.get("run_id"), type(event.get("run_id")))
    if db_path:
        try:
            conn = get_connection(db_path)
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
                return {"status": "ok"}
        except Exception as e1:
            try:
                if not os.path.exists(str(db_path)):
                    init_db(str(db_path), True)
                conn = sqlite3.connect(str(db_path))
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
                return {"status": "ok"}
            except sqlite3.OperationalError as e:
                print("Error #1 - ingest", e1)
                print("SQLite Error - ingest", e)
                print("SQLite Error - ingest vals", vals)
            except Exception as e:
                print("Error #1 - ingest", e1)
                print("Error #2 - ingest", e)
                print("Error - ingest vals", vals)
    
    return {"status": "error"}


