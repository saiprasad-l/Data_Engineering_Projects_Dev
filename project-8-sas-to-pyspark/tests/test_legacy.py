"""The oracle's SAS rules, pinned against behaviour documented for SAS."""
import math

import pandas as pd

from src import legacy as L


def test_missing_sorts_below_every_number():
    assert L.lt(math.nan, -1e300)          # . < any number
    assert not L.lt(math.nan, math.nan)
    assert not L.gt(math.nan, 90)          # . > 90 is false
    assert not L.ge(math.nan, 740)


def test_plus_propagates_missing_but_sum_statement_does_not():
    assert math.isnan(L.add(1, math.nan))
    assert L.sum_stmt(math.nan, 1) == 1
    assert L.sum_stmt(5, math.nan) == 5


def test_round_is_half_away_from_zero_with_fuzz():
    assert L.sas_round(2.675, 0.01) == 2.68     # binary 2.67499999... still rounds up
    assert L.sas_round(0.125, 0.01) == 0.13     # Python round() gives 0.12
    assert L.sas_round(-0.125, 0.01) == -0.13
    assert L.sas_round(1.004, 0.01) == 1.0
    assert math.isnan(L.sas_round(math.nan))


def test_max_ignores_missing():
    assert L.sas_max(0, math.nan) == 0
    assert L.sas_max(0, -5) == 0


def test_lag_is_a_queue_not_previous_row():
    lag = L.Lag()
    # conditional execution: only called on rows 2 and 4
    seen = []
    for i, v in enumerate([10, 20, 30, 40]):
        if i % 2 == 1:
            seen.append(lag(v))
    assert math.isnan(seen[0]) and seen[1] == 20   # not 30: the queue skipped row 3


def test_merge_by_is_not_a_cartesian_product():
    left = [{"k": 1, "a": "x"}, {"k": 1, "a": "y"}, {"k": 1, "a": "z"}]
    right = [{"k": 1, "b": "p"}, {"k": 1, "b": "q"}]
    rows = [(p["a"], p["b"]) for p, _, _ in L._merge_by(left, right, "k")]
    assert rows == [("x", "p"), ("y", "q"), ("z", "q")]     # 3 rows, not 6


def test_merge_by_last_dataset_wins_on_shared_column():
    left = [{"k": 1, "v": "left"}]
    right = [{"k": 1, "v": "right"}]
    [(pdv, a, b)] = list(L._merge_by(left, right, "k"))
    assert pdv["v"] == "right" and a and b


def test_first_last_flags():
    rows = [{"g": "A"}, {"g": "A"}, {"g": "B"}]
    flags = [(f, l) for _, f, l in L.first_last(rows, "g")]
    assert flags == [(True, False), (False, True), (True, True)]


def test_proc_means_drops_missing_class_values():
    df = pd.DataFrame({"g": ["A", "A", None], "v": [1.0, math.nan, 5.0]})
    groups = L._proc_means_nway(df, ["g"])
    assert list(groups) == [("A",)] and len(groups[("A",)]) == 2


def test_parity_treats_int_and_float_as_the_same_number():
    from src.parity import compare
    a = pd.DataFrame({"k": [1, 2], "n": [5, 7]})
    b = pd.DataFrame({"k": [1, 2], "n": [5.0, 7.0]})
    assert compare(a, b, key=["k"]).passed
