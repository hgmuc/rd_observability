import pandas as pd
import numpy as np
from rd_observability.observability import extract_metrics, Result

import pytest

# 👉 This runs the same test with multiple inputs.

@pytest.mark.parametrize("obj,expected_key", [
    ({"a":1}, "top_level_keys"),
    (pd.DataFrame({"x":[1]}), "rows"),
    (np.zeros((1,2)), "shape"),
    (Result(4,3), "data_cnt"),
    (Result(4,3), "error_cnt"),
    (Result(4,3), "rel_cnt"),
    ({"a":1}, "rel_cnt"),
    (np.zeros((1,2)), "dims"),
    (pd.DataFrame({"x":[1]}), "type"),
    (pd.DataFrame({"x":[1]}), "dims"),
])
def test_extract_metrics(obj, expected_key):
    m = extract_metrics(obj)
    print(m)
    assert expected_key in m
