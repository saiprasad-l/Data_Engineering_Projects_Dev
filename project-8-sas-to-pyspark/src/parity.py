"""Parity harness — prove a migrated dataset matches its legacy source.

A migration is not finished when the new job runs without error. It is finished
when the output is demonstrably identical to what the legacy process produced.
This module is the evidence.

Works on pandas DataFrames so it can run locally against the reference
implementation, and on Spark DataFrames via `.toPandas()` for modest result
sets (or `spark_checksums()` for large ones, which pushes the aggregation down).
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from decimal import Decimal

import numpy as np
import pandas as pd


@dataclass
class ParityReport:
    passed: bool
    row_count: tuple[int, int]
    missing_columns: list[str] = field(default_factory=list)
    extra_columns: list[str] = field(default_factory=list)
    checksum_mismatches: list[str] = field(default_factory=list)
    null_rate_deltas: dict[str, tuple[float, float]] = field(default_factory=dict)
    aggregate_deltas: dict[str, dict] = field(default_factory=dict)
    mismatched_rows: int = 0
    sample_diff: pd.DataFrame | None = None

    def __str__(self):
        lines = [f"{'PASS' if self.passed else 'FAIL'}  rows legacy={self.row_count[0]} migrated={self.row_count[1]}"]
        if self.missing_columns:
            lines.append(f"  missing columns : {self.missing_columns}")
        if self.extra_columns:
            lines.append(f"  extra columns   : {self.extra_columns}")
        if self.checksum_mismatches:
            lines.append(f"  checksum differs: {self.checksum_mismatches}")
        for col, (a, b) in self.null_rate_deltas.items():
            lines.append(f"  null-rate {col}: legacy={a:.4f} migrated={b:.4f}")
        for col, d in self.aggregate_deltas.items():
            lines.append(f"  agg {col}: {d}")
        if self.sample_diff is not None and len(self.sample_diff):
            lines.append(f"  rows that differ: {self.mismatched_rows:,} ({len(self.sample_diff)} shown)")
            lines.append(self.sample_diff.to_string(max_rows=10))
        return "\n".join(lines)


def _normalize(v) -> str:
    """Numbers compare by value whatever their type: 5, 5.0, np.int64(5) and
    Decimal('5.00') all fingerprint the same. Everything else by its text."""
    if isinstance(v, (bool, np.bool_)):
        v = int(v)
    if isinstance(v, (int, float, Decimal, np.integer, np.floating)):
        return f"{float(v):.6f}"
    return str(v)


def _checksum(series: pd.Series) -> str:
    """Order-independent column fingerprint. Values are normalized to strings so
    int64 vs float64 doesn't register as a difference when the values match —
    SAS and Spark disagree on numeric types more often than on numbers."""
    norm = series.dropna().map(_normalize)
    joined = "|".join(sorted(norm.astype(str).tolist()))
    return hashlib.sha256(joined.encode()).hexdigest()[:16]


def compare(
    legacy: pd.DataFrame,
    migrated: pd.DataFrame,
    key: list[str] | None = None,
    tolerance: float = 1e-6,
    null_rate_tolerance: float = 0.0,
) -> ParityReport:
    """Compare two DataFrames the way a migration reviewer would.

    key: columns forming the business key. When given, a sample of mismatched
    rows is produced by joining on it — far more useful for debugging than
    "the checksums differ".
    """
    legacy_cols, migrated_cols = set(legacy.columns), set(migrated.columns)
    report = ParityReport(
        passed=True,
        row_count=(len(legacy), len(migrated)),
        missing_columns=sorted(legacy_cols - migrated_cols),
        extra_columns=sorted(migrated_cols - legacy_cols),
    )
    if report.missing_columns or report.extra_columns or len(legacy) != len(migrated):
        report.passed = False

    for col in sorted(legacy_cols & migrated_cols):
        if _checksum(legacy[col]) != _checksum(migrated[col]):
            report.checksum_mismatches.append(col)
            report.passed = False

        a, b = legacy[col].isna().mean(), migrated[col].isna().mean()
        if abs(a - b) > null_rate_tolerance:
            report.null_rate_deltas[col] = (a, b)
            report.passed = False

        if pd.api.types.is_numeric_dtype(legacy[col]) and pd.api.types.is_numeric_dtype(migrated[col]):
            deltas = {}
            for fn in ("sum", "min", "max", "mean"):
                x, y = getattr(legacy[col], fn)(), getattr(migrated[col], fn)()
                if pd.isna(x) and pd.isna(y):
                    continue
                if pd.isna(x) or pd.isna(y) or abs(float(x) - float(y)) > tolerance:
                    deltas[fn] = (x, y)
            if deltas:
                report.aggregate_deltas[col] = deltas
                report.passed = False

    if key and not report.missing_columns:
        diff = _keyed_diff(legacy, migrated, key, tolerance)
        if len(diff):
            report.mismatched_rows = int(diff.attrs["mismatched_rows"])
            report.sample_diff = diff.head(20)
            report.passed = False

    return report


def _keyed_diff(legacy: pd.DataFrame, migrated: pd.DataFrame, key: list[str], tolerance: float) -> pd.DataFrame:
    """Rows that differ, joined on the business key: keys present on one side
    only, and keys whose values disagree - showing just the columns that differ."""
    def key_text(v):
        if pd.isna(v):
            return None
        if isinstance(v, (int, float, np.integer, np.floating)) and float(v).is_integer():
            return str(int(v))
        return _normalize(v)

    def norm_keys(df):
        df = df.copy()
        for k in key:
            df[k] = df[k].map(key_text)
        return df

    merged = norm_keys(legacy).merge(norm_keys(migrated), on=key, how="outer",
                                     suffixes=("_legacy", "_migrated"), indicator=True)
    value_cols = [c for c in legacy.columns if c in migrated.columns and c not in key]
    bad = pd.Series(False, index=merged.index)
    differs = {}
    for c in value_cols:
        a, b = merged[f"{c}_legacy"], merged[f"{c}_migrated"]
        both_missing = a.isna() & b.isna()
        if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
            same = both_missing | ((a - b).abs() <= tolerance)
        else:
            same = both_missing | (a.map(lambda v: None if pd.isna(v) else _normalize(v))
                                   == b.map(lambda v: None if pd.isna(v) else _normalize(v)))
        differs[c] = ~same & (merged["_merge"] == "both")
        bad |= differs[c]
    bad |= merged["_merge"] != "both"

    rows = merged[bad]
    cols = [c for c in value_cols if differs[c].any()]
    show = key + ["_merge"] + [f"{c}_{side}" for c in cols for side in ("legacy", "migrated")]
    out = rows[show].rename(columns={"_merge": "found_in"})
    out["found_in"] = out["found_in"].astype(str).map(
        {"left_only": "legacy only", "right_only": "migrated only", "both": "both"})
    out.attrs["mismatched_rows"] = int(bad.sum())
    return out


def spark_checksums(df, columns: list[str] | None = None) -> dict[str, str]:
    """Column fingerprints computed in Spark, for datasets too large to collect.
    Same idea as _checksum but the sort/hash happens on the cluster."""
    from pyspark.sql import functions as F

    columns = columns or df.columns
    row = df.select([
        F.sha2(F.concat_ws("|", F.sort_array(F.collect_list(F.col(c).cast("string")))), 256).alias(c)
        for c in columns
    ]).collect()[0]
    return {c: row[c][:16] for c in columns}


def _reference(pattern: str, df: pd.DataFrame, against: str):
    """The answer key: real SAS output when exported, else the Python oracle."""
    from . import legacy, load

    if against == "auto":
        against = "sas" if load.has_expected(pattern) else "python"
    if against == "sas":
        return "real SAS", load.expected(pattern)
    return "Python oracle", legacy.run(pattern, df)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Run parity for one pattern")
    ap.add_argument("--pattern", required=True, help="pattern id, e.g. 02")
    ap.add_argument("--against", choices=["auto", "sas", "python"], default="auto",
                    help="answer key: real SAS output in expected/, or the Python oracle "
                         "(auto = SAS when available)")
    ap.add_argument("--naive", action="store_true",
                    help="check a naive hand translation (src/naive.py) instead - it should FAIL")
    ap.add_argument("--validate-oracle", action="store_true",
                    help="compare the Python oracle with real SAS output instead")
    args = ap.parse_args()

    from . import legacy, load

    df = load.loans()
    key = load.KEYS.get(args.pattern)
    if args.validate_oracle:
        print("Python oracle vs real SAS")
        print(compare(load.expected(args.pattern), legacy.run(args.pattern, df), key=key))
    elif args.naive:
        from . import naive
        name, lhs = _reference(args.pattern, df, args.against)
        print(f"NAIVE PySpark vs {name}")
        print(compare(lhs, naive.run(args.pattern), key=key))
    else:
        name, lhs = _reference(args.pattern, df, args.against)
        print(f"converter PySpark vs {name}")
        print(compare(lhs, load.migrated(args.pattern, df), key=key))
