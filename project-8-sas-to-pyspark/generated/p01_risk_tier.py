"""PySpark generated from sas/01_risk_tier.sas by the sas2databricks converter.

Do not edit by hand. Regenerate with:
    python -m src.converter sas/01_risk_tier.sas

Conversion report
-----------------
[  CONVERTED] DATA work.loan_risk (lines 18-33)
              - `dti < 20`: SAS treats missing as smaller than any number, so a NULL satisfies it (plain Spark would say NULL -> false)
              - `dti < 35`: SAS treats missing as smaller than any number, so a NULL satisfies it (plain Spark would say NULL -> false)
"""
# ruff: noqa
import math
from datetime import date

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import sasrt as sas

INPUTS = ['loans']
OUTPUTS = ['loan_risk']


def run(spark: SparkSession, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    loans = tables["loans"]

    # ──── DATA work.loan_risk  (sas lines 18-33) ────
    # | data work.loan_risk;
    # |     set work.loans;
    # |     length risk_tier $6;
    # |
    # |     if fico_range_low >= 740 and dti < 20 then risk_tier = 'LOW';
    # |     else if fico_range_low >= 680 and dti < 35 then risk_tier = 'MEDIUM';
    # |     else risk_tier = 'HIGH';
    # |
    # |     if revol_util > 90 then high_util = 1;
    # |     else high_util = 0;
    # |
    # |     is_bad = (loan_status in ('Charged Off', 'Default'));
    # |
    # |     keep id grade fico_range_low dti revol_util loan_status
    # |          risk_tier high_util is_bad;
    # | run;
    df = loans
    df = df.withColumn("risk_tier", (
        F.when(((F.col("fico_range_low").isNotNull() & (F.col("fico_range_low") >= 740)) & (F.col("dti").isNull() | (F.col("dti") < 20))), 'LOW')
        .when(((F.col("fico_range_low").isNotNull() & (F.col("fico_range_low") >= 680)) & (F.col("dti").isNull() | (F.col("dti") < 35))), 'MEDIUM')
        .otherwise('HIGH')
    ))
    df = df.withColumn("high_util", (
        F.when((F.col("revol_util").isNotNull() & (F.col("revol_util") > 90)), 1)
        .otherwise(0)
    ))
    df = df.withColumn("is_bad", (F.col("loan_status").isNotNull() & F.col("loan_status").isin('Charged Off', 'Default')).cast('int'))
    loan_risk = df.select("id", "grade", "fico_range_low", "dti", "revol_util", "loan_status", "risk_tier", "high_util", "is_bad")

    return {"loan_risk": loan_risk}
