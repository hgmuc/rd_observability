from rd_observability.obs_classes import Result

def test_result_to_dict():
    r = Result(data_cnt=10, error_cnt=1)
    r.add_metric("foo", 123)
    d = r.to_dict()

    assert d["rel_cnt"] == 11
    assert d["data_cnt"] == 10
    assert d["foo"] == 123
    assert d["type"] == "Result"
