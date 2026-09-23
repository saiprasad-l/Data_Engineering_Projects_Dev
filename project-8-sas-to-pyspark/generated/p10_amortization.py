"""PySpark generated from sas/10_amortization.sas by the sas2databricks converter.

Do not edit by hand. Regenerate with:
    python -m src.converter sas/10_amortization.sas

Conversion report
-----------------
[  CONVERTED] DATA work.schedule (lines 20-29)
              - DO month_num = ... TO ...; OUTPUT; -> explode(sequence): one row per iteration (an empty range gives no rows, as in SAS)
[  CONVERTED] PROC SORT work.schedule (lines 31-33)
              - PROC SORT dropped: Spark DataFrames have no row order. Sort key (id, month_num) kept as metadata and used to order windows downstream.
[ SEQUENTIAL] DATA work.amortization (lines 35-47)
              - balance depends on its own previous computed value (self-referential RETAIN): no window function can express it. Each BY group is independent (reset on FIRST.), so groups run in parallel via groupBy(id).applyInPandas and rows are walked in (id, month_num) order
"""
# ruff: noqa
import math
from datetime import date

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import sasrt as sas

INPUTS = ['loans']
OUTPUTS = ['schedule', 'amortization']


def run(spark: SparkSession, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    loans = tables["loans"]

    # ──── DATA work.schedule  (sas lines 20-29) ────
    # | data work.schedule;
    # |     set work.loans(where=(issue_d >= '01OCT2018'd and grade = 'A'));
    # |     monthly_rate = int_rate / 100 / 12;
    # |
    # |     do month_num = 1 to term;
    # |         output;
    # |     end;
    # |
    # |     keep id month_num term monthly_rate installment funded_amnt;
    # | run;
    df = loans.where(((F.col("issue_d").isNotNull() & (F.col("issue_d") >= date(2018, 10, 1))) & F.col("grade").eqNullSafe('A')))
    df = df.withColumn("monthly_rate", ((F.col("int_rate") / 100) / 12))
    df = df.withColumn("month_num", F.explode(sas.do_to(F.lit(1), F.col("term"))))
    schedule = df.select("id", "month_num", "term", "monthly_rate", "installment", "funded_amnt")

    # ──── PROC SORT work.schedule  (sas lines 31-33) ────
    # | proc sort data=work.schedule;
    # |     by id month_num;
    # | run;
    # schedule: sort key carried forward: ['id', 'month_num']

    # ──── DATA work.amortization  (sas lines 35-47) ────
    # | data work.amortization;
    # |     set work.schedule;
    # |     by id;
    # |     retain balance;
    # |
    # |     if first.id then balance = funded_amnt;
    # |
    # |     interest  = round(balance * monthly_rate, 0.01);
    # |     principal = round(installment - interest, 0.01);
    # |     balance   = max(0, round(balance - principal, 0.01));
    # |
    # |     keep id month_num interest principal balance;
    # | run;
    df = schedule
    def _amortization_rows(pdf: pd.DataFrame) -> pd.DataFrame:
        rows = pdf.sort_values(['id', 'month_num'], kind="mergesort").to_dict("records")
        out, pdv = [], {}
        for i, row in enumerate(rows):
            # next iteration: read a row; computed variables start missing, RETAINed ones carry over
            pdv = {**row, "interest": math.nan, "principal": math.nan, "balance": pdv.get("balance", math.nan)}
            first_id = i == 0 or any(rows[i - 1][c] != row[c] for c in ['id'])
            if first_id:
                pdv["balance"] = pdv["funded_amnt"]
            pdv["interest"] = sas.py_round((pdv["balance"] * pdv["monthly_rate"]), 0.01)
            pdv["principal"] = sas.py_round((pdv["installment"] - pdv["interest"]), 0.01)
            pdv["balance"] = sas.py_max(0, sas.py_round((pdv["balance"] - pdv["principal"]), 0.01))
            out.append({c: pdv[c] for c in ['id', 'month_num', 'interest', 'principal', 'balance']})  # implicit OUTPUT
        return pd.DataFrame(out, columns=['id', 'month_num', 'interest', 'principal', 'balance'])
    amortization = df.groupBy("id").applyInPandas(_amortization_rows, schema="id bigint, month_num int, interest double, principal double, balance double")

    return {"schedule": schedule, "amortization": amortization}
