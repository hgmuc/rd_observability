import rd_observability.observability as observability
from rd_observability.observability_db import get_connection 
import pickle

def test_do_pickle(temp_db, monkeypatch, tmp_path):
    monkeypatch.setattr(observability, "DB_PATH", temp_db)

    obj = {"x":1}
    f = tmp_path / "obj.pkl"

    observability.do_pickle(obj, f)

    assert f.exists()
    assert pickle.load(open(f,"rb")) == obj

# Regression test to verify that a fix 'fname -> str(fname)' was actually solved 
# and did not break anything
def test_fname_path_is_converted(temp_db, monkeypatch, tmp_path):
    monkeypatch.setattr(observability, "DB_PATH", temp_db)
    observability.set_run_params()
    
    f = tmp_path / "x.pkl"
    f.write_bytes(b"123")

    observability.record_event({}, f, "pickle", {'reg_key': 'WNW'}, db_path=temp_db)

    conn = get_connection(temp_db)
    assert conn
    cur = conn.cursor()
    cur.execute("SELECT filename FROM artifacts")
    fname = cur.fetchone()[0]

    assert isinstance(fname, str)

