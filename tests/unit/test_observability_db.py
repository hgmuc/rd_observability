import sqlite3
import rd_observability.observability_db
import rd_observability.obs_conf

def test_schema_exists():
    assert rd_observability.observability_db.SCHEMA
    assert isinstance(rd_observability.observability_db.SCHEMA, str)

def test_patches_dict_exists():
    assert rd_observability.observability_db.PATCHES
    assert isinstance(rd_observability.observability_db.PATCHES, dict)
    assert len(rd_observability.observability_db.PATCHES) > 1

def test_init_db_creates_tables(temp_db):
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}

    assert "artifacts" in tables
    assert "schema_migrations" in tables


def test_init_db_creates_views(temp_db):
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='view'")
    views = {r[0] for r in cur.fetchall()}

    assert "baseline_lvl1" in views
    assert "baseline_lvl2" in views

    
def test_patches_applied(temp_db, tmp_path):
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    cur.execute("SELECT version FROM schema_migrations")
    #print(cur.fetchall())
    versions = {r[0] for r in cur.fetchall()}
    print("versions", versions)
    print("patches", list(rd_observability.observability_db.PATCHES))

    assert max(versions) == max(rd_observability.observability_db.PATCHES)
    assert len(versions) == len(rd_observability.observability_db.PATCHES)


def test_get_connection(tmp_path):
    #assert rd_observability.obs_conf.CHECKED_PATCHES == False
    #if not rd_observability.obs_conf.CHECKED_PATCHES:
    #    rd_observability.observability_db.init_db(tmp_path / "test_obs.sqlite")
    #    print("INIT_DB DONE", rd_observability.obs_conf.CHECKED_PATCHES)

    conn = rd_observability.observability_db.get_connection(tmp_path / "test_obs.sqlite")
    if conn:
        cur = conn.cursor()

        cur.execute("SELECT version FROM schema_migrations")
        #print(cur.fetchall())
        versions = {r[0] for r in cur.fetchall()}
        print("versions", versions)
        print("patches", list(rd_observability.observability_db.PATCHES))

        assert max(versions) == max(rd_observability.observability_db.PATCHES)
        assert len(versions) == len(rd_observability.observability_db.PATCHES)
    
    conn = rd_observability.observability_db.get_connection(tmp_path / "test_obs.sqlite")
    if conn:
        cur = conn.cursor()

        cur.execute("SELECT version FROM schema_migrations")
        #print(cur.fetchall())
        versions = {r[0] for r in cur.fetchall()}
        print("versions", versions)
        print("patches", list(rd_observability.observability_db.PATCHES))
    
 