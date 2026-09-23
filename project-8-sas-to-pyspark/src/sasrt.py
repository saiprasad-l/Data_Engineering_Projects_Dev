"""SAS runtime for generated PySpark: the SAS rules Spark doesn't have built in.

Generated code imports this as `sas`. Two halves:
  - Column helpers, for vectorised steps
  - py_* helpers, for the sequential (applyInPandas) fallback

Written independently of src/legacy.py (the oracle) on purpose - parity is
only evidence if the migration and the answer key don't share code.
"""
from __future__ import annotations

import math
import operator
import sys
from functools import reduce

from pyspark import cloudpickle
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Ship this module's code with any pandas UDF that uses it, so Spark workers
# don't need the project on their import path (true on Databricks serverless).
cloudpickle.register_pickle_by_value(sys.modules[__name__])

# SAS ROUND treats values within this fraction of a rounding unit of the
# half-way point as exactly half (e.g. 2.675 is 2.67499999... in binary).
ROUND_FUZZ = 1e-7

# --------------------------------------------------------------------------
# Column helpers
# --------------------------------------------------------------------------


def _c(x) -> Column:
    return x if isinstance(x, Column) else F.lit(x)


def lt(a, b) -> Column:
    """a < b where missing sorts below every value (never returns NULL)."""
    a, b = _c(a), _c(b)
    return F.coalesce(a < b, a.isNull() & b.isNotNull())


def gt(a, b) -> Column:
    return lt(b, a)


def le(a, b) -> Column:
    return ~gt(a, b)


def ge(a, b) -> Column:
    return ~lt(a, b)


def div(a, b) -> Column:
    """a / b; division by zero gives missing (SAS logs a note), not an error
    (Spark in ANSI mode, the default on Databricks, raises)."""
    a, b = _c(a), _c(b)
    return F.when(b != 0, a / b)


def round_(x, unit=1) -> Column:
    """SAS ROUND(x, unit): nearest multiple of unit, halves away from zero.
    F.round works on the exact binary value, so it can round 2.675 down."""
    x = _c(x)
    n = F.floor(F.abs(x) / unit + 0.5 + ROUND_FUZZ)
    return F.round(F.signum(x) * n * unit, 10)


def sum_(*args) -> Column:
    """SAS SUM(a, b, ...): ignores missing arguments; missing only if all are."""
    cols = [_c(a) for a in args]
    any_present = reduce(operator.or_, [c.isNotNull() for c in cols])
    total = reduce(operator.add, [F.coalesce(c, F.lit(0)) for c in cols])
    return F.when(any_present, total)


def do_to(start, stop, step=1) -> Column:
    """Values of `DO i = start TO stop [BY step]`, as an array to explode.

    F.sequence(1, 0) counts DOWN to [1, 0]; a SAS loop with stop < start runs
    zero times. The empty array (and explode dropping it) keeps that."""
    start, stop = _c(start).cast("int"), F.floor(_c(stop)).cast("int")
    return F.when(stop >= start, F.sequence(start, stop, F.lit(step)))\
            .otherwise(F.array().cast("array<int>"))


def assert_no_many_to_many(left: DataFrame, right: DataFrame, by: list[str], step: str) -> None:
    """MERGE pairs rows within a BY group; a join multiplies them. The two agree
    unless BOTH sides repeat a BY value - stop rather than silently differ."""
    def repeats(df):
        return df.groupBy(*by).count().where(F.col("count") > 1).limit(1).count() > 0

    if repeats(left) and repeats(right):
        raise ValueError(
            f"{step}: both MERGE inputs repeat BY values {by}. SAS pairs such rows "
            "one-to-one; a join would multiply them. Needs manual migration.")


# --------------------------------------------------------------------------
# Python helpers (sequential fallback)
# --------------------------------------------------------------------------


def py_missing(v) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v)) or (isinstance(v, str) and not v.strip())


def py_lt(a, b) -> bool:
    if py_missing(a):
        return not py_missing(b)
    return False if py_missing(b) else a < b


def py_gt(a, b) -> bool:
    return py_lt(b, a)


def py_le(a, b) -> bool:
    return not py_gt(a, b)


def py_ge(a, b) -> bool:
    return not py_lt(a, b)


def py_eq(a, b) -> bool:
    if py_missing(a) or py_missing(b):
        return py_missing(a) and py_missing(b)
    return a == b


def py_ne(a, b) -> bool:
    return not py_eq(a, b)


def py_true(v) -> bool:
    """SAS truth: any value other than 0 and missing."""
    if isinstance(v, bool):
        return v
    return not py_missing(v) and v != 0


def py_div(a, b) -> float:
    return math.nan if py_missing(a) or py_missing(b) or b == 0 else a / b


def py_round(x, unit=1) -> float:
    if py_missing(x):
        return math.nan
    n = math.floor(abs(x) / unit + 0.5 + ROUND_FUZZ)
    return round(math.copysign(n * unit, x), 10)


def py_max(*args) -> float:
    vals = [a for a in args if not py_missing(a)]
    return max(vals) if vals else math.nan


def py_min(*args) -> float:
    vals = [a for a in args if not py_missing(a)]
    return min(vals) if vals else math.nan


def py_sum(*args) -> float:
    vals = [a for a in args if not py_missing(a)]
    return sum(vals) if vals else math.nan


def py_sum_stmt(acc, incr) -> float:
    return (0 if py_missing(acc) else acc) + (0 if py_missing(incr) else incr)


def py_do_range(start, stop, step=1):
    i = start
    while (step > 0 and i <= stop) or (step < 0 and i >= stop):
        yield i
        i += step
