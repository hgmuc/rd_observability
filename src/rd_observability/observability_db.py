from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Union, Set, Literal, TypeAlias
#from .obs_conf import DB_PATH, CHECKED_PATCHES
import rd_observability.obs_conf

SQLiteType: TypeAlias = Literal["INTEGER", "REAL", "TEXT", "BLOB", "NUMERIC"]
SQLStatement: TypeAlias = str

SCHEMA: SQLStatement = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT
);

CREATE TABLE IF NOT EXISTS artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    key TEXT,
    cell TEXT,
    object_type TEXT,
    reg_key TEXT,
    lvl TEXT,
    filename TEXT,
    serializer TEXT,
    size_bytes INTEGER,
    script_name TEXT,
    pipeline_version TEXT,
    run_id INTEGER,
    metrics_json TEXT,
    fingerprint TEXT
);

CREATE INDEX IF NOT EXISTS idx_cell ON artifacts(cell);
CREATE INDEX IF NOT EXISTS idx_object_type ON artifacts(object_type);
CREATE INDEX IF NOT EXISTS idx_reg_key ON artifacts(reg_key);
CREATE INDEX IF NOT EXISTS idx_timestamp ON artifacts(timestamp);

CREATE VIEW IF NOT EXISTS baseline_lvl1 AS SELECT 
    object_type, count(*) FROM artifacts WHERE lvl = '1' and run_id = 0 GROUP BY object_type;
CREATE VIEW IF NOT EXISTS baseline_lvl2 AS SELECT 
    object_type, count(*) FROM artifacts WHERE lvl = '2' and run_id = 0 GROUP BY object_type;
"""

''' EXAMPLE / TEMPLATE for Patch scripts
PATCHES = {
    1: """
    ALTER TABLE artifacts
    ADD COLUMN count INTEGER
    GENERATED ALWAYS AS (json_extract(metrics_json, '$.count')) VIRTUAL;
    """,

    2: """
    ALTER TABLE artifacts
    ADD COLUMN admin_level_max INTEGER
    GENERATED ALWAYS AS (json_extract(metrics_json, '$.admin_level_max')) VIRTUAL;
    """,

    3: """
    CREATE INDEX IF NOT EXISTS idx_count ON artifacts(count);
    """
}
'''

PATCHES: dict[int, SQLStatement] = {1: """
    ALTER TABLE artifacts
    ADD COLUMN rel_count INTEGER
    GENERATED ALWAYS AS (json_extract(metrics_json, '$.rel_cnt')) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN nan_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.nan_count'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN inf_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.inf_count'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN polygon_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.geom_types.Polygon'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN multi_polygon_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.geom_types.MultiPolygon'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN line_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.geom_types.LineString'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN multi_line_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.geom_types.MultiLineString'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN point_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.geom_types.Point'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN gcoll_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.geom_types.GeometryCollection'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN empty_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.geom_empty_count'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN invalid_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.geom_invalid_count'), 0)) VIRTUAL;
    """, 
    2: """
    ALTER TABLE artifacts
    ADD COLUMN csv_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.csv'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN txt_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.txt'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN zip_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.zip'), 0)) VIRTUAL;
    
    ALTER TABLE artifacts
    ADD COLUMN gzip_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.gzip'), 0)) VIRTUAL;
        
    ALTER TABLE artifacts
    ADD COLUMN pkl_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.pkl'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN jpg_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.jpg'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN webp_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.webp'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN png_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.png'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN avif_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.avif'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN md_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.md'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN gpx_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.gpx'), 0)) VIRTUAL;

    ALTER TABLE artifacts
    ADD COLUMN html_count INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.file_types.html'), 0)) VIRTUAL;
    """, 
    3: """
    ALTER TABLE artifacts
    ADD COLUMN dims INTEGER
    GENERATED ALWAYS AS ( COALESCE(json_extract(metrics_json, '$.dims'), 0)) VIRTUAL;

    UPDATE artifacts
    SET metrics_json = json_set(metrics_json, '$.dims', 2)
    WHERE serializer = 'baseline df';

    UPDATE artifacts
    SET metrics_json = json_set(
        metrics_json,
        '$.shape',
        json_array(
            json_extract(metrics_json, '$.rows'),
            json_extract(metrics_json, '$.columns')
        )
    )
    WHERE serializer = 'baseline df';    
    """, 
}  # see example in the comment above

# Advanced pattern - Instead of numbered patches, you can auto-promote metrics, 
# define AUTO_COLUMNS, then loop and ALTER TABLE dynamically
AUTO_COLUMNS: dict[str, SQLiteType] = {
    "count": "INTEGER",
    "admin_level_max": "INTEGER",
    "bbox_area": "REAL"
}  # currently not active

def apply_patches(conn: sqlite3.Connection) -> None:
    try:
        cur: sqlite3.Cursor = conn.cursor()

        # Ensure migration table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT
            )
        """)

        # Get applied versions
        cur.execute("SELECT version FROM schema_migrations")
        applied: Set[int] = {row[0] for row in cur.fetchall()}

        # Apply missing patches in order
        for version in sorted(PATCHES):
            if version in applied:
                continue

            print(f"Applying DB patch {version}")
            cur.executescript(PATCHES[version])
            cur.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, datetime('now'))",
                (version,)
            )

        conn.commit()
    except Exception as e:
        print("Error - apply_patches", e)

    return None
        

def init_db(db_path: Union[Path, str], do_patch: bool = True) -> None:
    #print("init_db")
    conn: sqlite3.Connection = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    #print("init_db - initial script done")
    conn.commit()
    if do_patch:
        apply_patches(conn)

    conn.close()

def get_connection(db_path: Union[Path, None] = None) -> sqlite3.Connection | None:
    db_path = db_path or rd_observability.obs_conf.DB_PATH
    try:
        if not db_path.exists():
            init_db(db_path, False)
        
        conn: sqlite3.Connection = sqlite3.connect(db_path)

        apply_patches(conn)
        
        print("done applying patches")
        return conn
    except Exception as e:
        print("Could not connect to DB", e)
        return None


if __name__ == "__main__":
    init_db(rd_observability.obs_conf.DB_PATH)


