#import tempfile
#import sqlite3
import sys
import pytest
from pathlib import Path

import rd_observability.observability_db
import rd_observability.obs_conf

#sys.path.append("..")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    # Override DB_PATH globally
    db = tmp_path / "test_obs.sqlite"
    monkeypatch.setattr(rd_observability.obs_conf, "DB_PATH", db)
    rd_observability.observability_db.init_db(db)
    return db

# 👉 tmp_path = pytest built-in temp folder
# 👉 monkeypatch = override global variables safely


@pytest.fixture(scope="session")
def shared_db(tmp_path_factory, monkeypatch):
    db_path = tmp_path_factory.mktemp("obs_data") / "test_obs.sqlite"

    # monkeypatch globally
    import rd_observability.observability
    import rd_observability.observability_db
    monkeypatch.setattr(rd_observability.observability, "DB_PATH", db_path)

    # init schema once
    rd_observability.observability_db.init_db(db_path)

    return db_path


@pytest.fixture(scope="session")
def integration_db(monkeypatch):
    #db_path = Path("tests") / "inttest_obs.sqlite"
    db_path = Path(__file__).parent / "inttest_obs.sqlite"  # same as above, but less prone to breaking

    # monkeypatch globally
    import observability
    import rd_observability.observability_db
    monkeypatch.setattr(observability, "DB_PATH", db_path)

    # init schema once
    rd_observability.observability_db.init_db(db_path)

    return db_path
