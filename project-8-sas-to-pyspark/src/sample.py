"""Build the fixed 100k-row LendingClub sample every other step runs on.

Input : the Kaggle file `accepted_2007_to_2018Q4.csv(.gz)` (~2.2M loans, 151 cols)
Output: data/sample/loans_sample.csv — the ONE file that both SAS and Spark read.

Only light, loss-free normalisation happens here, so SAS and Spark start from
byte-identical input:
  - keep the columns the patterns use
  - issue_d "Dec-2015" -> 2015-12-01 (ISO, imports cleanly in both SAS and Spark)
  - term " 36 months" -> 36
  - int_rate / revol_util: strip a trailing "%" if present
  - drop the summary/footer rows Kaggle's file carries (no loan_amnt)
Missing values are deliberately left as-is: how SAS and Spark treat them
differently is one of the traps the project demonstrates.

    python -m src.sample --input ~/Downloads/accepted_2007_to_2018Q4.csv.gz
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

COLUMNS = [
    "id", "loan_amnt", "funded_amnt", "term", "int_rate", "installment",
    "grade", "sub_grade", "emp_length", "home_ownership", "annual_inc",
    "verification_status", "issue_d", "loan_status", "purpose", "addr_state",
    "dti", "fico_range_low", "fico_range_high", "delinq_2yrs", "revol_util",
    "total_pymnt",
]
DEFAULT_OUT = Path("data/sample/loans_sample.csv")
SEED = 42


def _strip_pct(s: pd.Series) -> pd.Series:
    if s.dtype == object:
        s = pd.to_numeric(s.str.rstrip("%").str.strip(), errors="coerce")
    return s


def build(input_path: Path, n: int = 100_000, seed: int = SEED) -> pd.DataFrame:
    df = pd.read_csv(input_path, usecols=COLUMNS, low_memory=False)
    df = df[df["loan_amnt"].notna()]

    df = df.sample(n=min(n, len(df)), random_state=seed)

    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["issue_d"] = pd.to_datetime(df["issue_d"], format="%b-%Y").dt.strftime("%Y-%m-%d")
    df["term"] = df["term"].str.extract(r"(\d+)", expand=False).astype("Int64")
    df["int_rate"] = _strip_pct(df["int_rate"])
    df["revol_util"] = _strip_pct(df["revol_util"])

    # Stable row order: SAS BY-group logic needs sorted input anyway, and a fixed
    # order makes diffs between runs meaningful.
    return df[COLUMNS].sort_values(["issue_d", "id"]).reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--input", required=True, type=Path, help="Kaggle accepted_*.csv or .csv.gz")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--n", type=int, default=100_000)
    args = ap.parse_args()

    df = build(args.input.expanduser(), n=args.n)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)

    print(f"wrote {len(df):,} rows x {df.shape[1]} cols -> {args.out}")
    print(f"issue_d range : {df['issue_d'].min()} .. {df['issue_d'].max()}")
    nulls = df.isna().mean()
    print("null rate     :", ", ".join(f"{c}={r:.1%}" for c, r in nulls[nulls > 0].items()) or "none")


if __name__ == "__main__":
    main()
