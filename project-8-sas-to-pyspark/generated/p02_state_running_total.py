"""PySpark generated from sas/02_state_running_total.sas by the sas2databricks converter.

Do not edit by hand. Regenerate with:
    python -m src.converter sas/02_state_running_total.sas

Conversion report
-----------------
[  CONVERTED] PROC SORT work.loans -> work.loans_by_state (lines 18-20)
              - PROC SORT dropped: Spark DataFrames have no row order. Sort key (addr_state, issue_d, id) kept as metadata and used to order windows downstream.
[  CONVERTED] DATA work.state_running (lines 22-36)
              - RETAIN cum_funded; cum_funded = cum_funded + ... (reset on FIRST.addr_state) -> running SUM over a ROWS window ordered by the full sort key; `+` keeps a missing total missing for the rest of the group
              - `cum_loans + 1` (sum statement, implied RETAIN, reset on FIRST.addr_state) -> row_number()
"""
# ruff: noqa
import math
from datetime import date

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import sasrt as sas

INPUTS = ['loans']
OUTPUTS = ['loans_by_state', 'state_running']


def run(spark: SparkSession, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    loans = tables["loans"]

    # ──── PROC SORT work.loans -> work.loans_by_state  (sas lines 18-20) ────
    # | proc sort data=work.loans out=work.loans_by_state;
    # |     by addr_state issue_d id;
    # | run;
    loans_by_state = loans  # sort key carried forward: ['addr_state', 'issue_d', 'id']

    # ──── DATA work.state_running  (sas lines 22-36) ────
    # | data work.state_running;
    # |     set work.loans_by_state;
    # |     by addr_state;
    # |     retain cum_funded 0;
    # |
    # |     if first.addr_state then do;
    # |         cum_funded = 0;
    # |         cum_loans = 0;
    # |     end;
    # |
    # |     cum_funded = cum_funded + funded_amnt;
    # |     cum_loans + 1;
    # |
    # |     keep addr_state issue_d id funded_amnt cum_funded cum_loans;
    # | run;
    df = loans_by_state
    w_addr_state_run = Window.partitionBy("addr_state").orderBy("issue_d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)  # ROWS frame: one row at a time, like SAS. RANGE would give tied rows one total
    df = df.withColumn("cum_funded", (
        F.when(F.max(F.col("funded_amnt").isNull().cast('int')).over(w_addr_state_run) == 1, F.lit(None))
        .otherwise(F.sum(F.col("funded_amnt")).over(w_addr_state_run))
    ))
    w_addr_state = Window.partitionBy("addr_state").orderBy("issue_d", "id")
    df = df.withColumn("cum_loans", F.row_number().over(w_addr_state))
    state_running = df.select("addr_state", "issue_d", "id", "funded_amnt", "cum_funded", "cum_loans")

    return {"loans_by_state": loans_by_state, "state_running": state_running}
