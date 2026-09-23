"""PySpark generated from sas/05_portfolio_summary.sas by the sas2databricks converter.

Do not edit by hand. Regenerate with:
    python -m src.converter sas/05_portfolio_summary.sas

Conversion report
-----------------
[  CONVERTED] PROC MEANS work.loans -> work.portfolio_summary (lines 17-27)
              - rows with a missing CLASS value (grade, emp_length) dropped, as PROC MEANS does without MISSING; Spark's groupBy would keep a NULL group
              - N(var) -> F.count(var) (non-missing); _FREQ_ -> F.count(*) (rows)
"""
# ruff: noqa
import math
from datetime import date

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import sasrt as sas

INPUTS = ['loans']
OUTPUTS = ['portfolio_summary']


def run(spark: SparkSession, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    loans = tables["loans"]

    # ──── PROC MEANS work.loans -> work.portfolio_summary  (sas lines 17-27) ────
    # | proc means data=work.loans noprint nway;
    # |     class grade emp_length;
    # |     var funded_amnt int_rate dti;
    # |     output out=work.portfolio_summary(drop=_type_ rename=(_freq_=n_loans))
    # |            sum(funded_amnt) = total_funded
    # |            mean(int_rate)   = avg_rate
    # |            min(int_rate)    = min_rate
    # |            max(int_rate)    = max_rate
    # |            n(dti)           = n_dti
    # |            mean(dti)        = avg_dti;
    # | run;
    portfolio_summary = (
        loans
        .where(F.col("grade").isNotNull() & F.col("emp_length").isNotNull())  # CLASS rows with missing values are excluded
        .groupBy("grade", "emp_length")
        .agg(
            F.count(F.lit(1)).alias("n_loans"),  # _FREQ_: all rows in the group
            F.sum("funded_amnt").alias("total_funded"),
            F.avg("int_rate").alias("avg_rate"),
            F.min("int_rate").alias("min_rate"),
            F.max("int_rate").alias("max_rate"),
            F.count("dti").alias("n_dti"),  # N: non-missing values only
            F.avg("dti").alias("avg_dti"),
        )
    )

    return {"portfolio_summary": portfolio_summary}
