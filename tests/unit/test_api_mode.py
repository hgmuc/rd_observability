# Ensures no SQLite write when OBS_MODE='api'.

import pytest
import rd_observability.observability
from rd_observability.observability import Result, record_event, set_run_params
from rd_observability.observability_db import get_connection
from rd_observability.obs_api import ingest

set_run_params()

def test_record_event_api(monkeypatch, tmp_path):
    monkeypatch.setattr(rd_observability.observability, "OBS_MODE", "api")

    # fake requests.post
    calls = []
    def fake_post(url, json, timeout):
        calls.append(json)

    monkeypatch.setattr(rd_observability.observability.requests, "post", fake_post)

    f = tmp_path / "x.bin"
    f.write_bytes(b"1")

    record_event(Result(), str(f), "pickle", meta={}, obs_api_url="http://fake")

    assert len(calls) == 1


@pytest.fixture
def capture_post(monkeypatch):
    calls = []
    def fake_post(url, json, timeout):
        calls.append(json)
    monkeypatch.setattr(rd_observability.observability.requests, "post", fake_post)
    return calls

def test_record_event_api_and_ingest(temp_db, capture_post, monkeypatch, tmp_path):
    # Force API mode
    monkeypatch.setattr(rd_observability.observability, "OBS_MODE", "api")

    # Create dummy file
    f = tmp_path / "x.bin"
    f.write_bytes(b"1")

    # Run record_event()
    record_event(Result(), str(f), "pickle", meta={}, obs_api_url="http://fake")

    # Ensure API call happened => capture_post IS ALREADY THE LIST of recorded posts
    assert len(capture_post) == 1

    # Extract payload dict
    payload = capture_post[0]
    assert isinstance(payload, dict)

    # Use a temp SQLite DB for ingest()
    # db_path = tmp_path / "obs.db"

    # Call ingest()
    print("PAYLOAD", payload)
    ingest(payload, db_path=temp_db)
    # Verify DB row was inserted
    rows = []
    conn = get_connection(temp_db)
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM artifacts")
        rows = cur.fetchall()
        conn.close()

    assert len(rows) == 1    

def test_record_event_api_and_ingest_extended(temp_db, capture_post, monkeypatch, tmp_path):
    # Force API mode
    monkeypatch.setattr(rd_observability.observability, "OBS_MODE", "api")

    # Create dummy file
    f = tmp_path / "x.bin"
    f.write_bytes(b"1")

    # Run record_event()
    record_event(Result(data_cnt=111, error_cnt=2), str(f), "pickle", 
                 meta={'cell': 'AB', 'reg_key': 'WNW', 'run_id': 245, 'script_name': 'test_script_api.py'}, 
                 obs_api_url="http://fake")

    # Ensure API call happened => capture_post IS ALREADY THE LIST of recorded posts
    assert len(capture_post) == 1

    # Extract payload dict
    payload = capture_post[0]
    print("PAYLOAD", payload)
    assert isinstance(payload, dict)
    assert payload['metrics_json'].get("rel_cnt") == 113
    assert payload['metrics_json'].get("data_cnt") == 111
    assert payload['metrics_json'].get("error_cnt") == 2
    assert payload.get("run_id") == 245
    assert payload.get("script_name") == "test_script_api.py"

    # Use a temp SQLite DB for ingest()
    # db_path = tmp_path / "obs.db"

    # Call ingest()
    print("PAYLOAD", payload)
    ingest(payload, db_path=temp_db)
    # Verify DB row was inserted
    rows = []
    conn = get_connection(temp_db)
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM artifacts")
        rows = cur.fetchall()
        conn.close()

    assert len(rows) == 1    


## Older version - less clean
def test_record_event_api_and_ingest_old(temp_db, monkeypatch, tmp_path):
    # Force API mode
    monkeypatch.setattr(rd_observability.observability, "OBS_MODE", "api")

    # Capture POST payload
    calls = []
    def fake_post(url, json, timeout):
        calls.append(json)
        class DummyResp:
            status_code = 200
        return DummyResp()

    monkeypatch.setattr(rd_observability.observability.requests, "post", fake_post)

    # Create dummy file
    f = tmp_path / "x.bin"
    f.write_bytes(b"1")

    # Run record_event()
    record_event(Result(), str(f), "pickle", meta={}, obs_api_url="http://fake")

    # Ensure API call happened
    assert len(calls) == 1

    # Extract payload dict
    payload = calls[0]
    assert isinstance(payload, dict)

    # Use a temp SQLite DB for ingest()
    # db_path = tmp_path / "obs.db"

    # Call ingest()
    print("PAYLOAD", payload)
    ingest(payload, db_path=temp_db)
    # Verify DB row was inserted
    conn = get_connection(temp_db)
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM artifacts")
        rows = cur.fetchall()
        conn.close()

    assert len(rows) == 1