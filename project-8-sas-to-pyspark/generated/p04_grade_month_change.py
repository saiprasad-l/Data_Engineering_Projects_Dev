"""PySpark generated from sas/04_grade_month_change.sas by the sas2databricks converter.

Do not edit by hand. Regenerate with:
    python -m src.converter sas/04_grade_month_change.sas

Conversion report
-----------------
[  CONVERTED] PROC SUMMARY work.loans -> work.grade_month (lines 19-24)
              - rows with a missing CLASS value (grade, issue_d) dropped, as PROC SUMMARY does without MISSING; Spark's groupBy would keep a NULL group
[  CONVERTED] DATA work.grade_mom (lines 26-39)
              - LAG/DIF blanked on FIRST.grade -> F.lag over a window partitioned by grade (SAS calls LAG on every row, so it is the previous row)
"""
# ruff: noqa
import math
from datetime import date

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import sasrt as sas

INPUTS = ['loans']
OUTPUTS = ['grade_month', 'grade_mom']


def run(spark: SparkSession, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    loans = tables["loans"]

    # ──── PROC SUMMARY work.loans -> work.grade_month  (sas lines 19-24) ────
    # | proc summary data=work.loans nway;
    # |     class grade issue_d;
    # |     var funded_amnt;
    # |     output out=work.grade_month(drop=_type_ rename=(_freq_=n_loans))
    # |            sum=funded;
    # | run;
    grade_month = (
        loans
        .where(F.col("grade").isNotNull() & F.col("issue_d").isNotNull())  # CLASS rows with missing values are excluded
        .groupBy("grade", "issue_d")
        .agg(
            F.count(F.lit(1)).alias("n_loans"),  # _FREQ_: all rows in the group
            F.sum("funded_amnt").alias("funded"),
        )
    )

    # ──── DATA work.grade_mom  (sas lines 26-39) ────
    # | data work.grade_mom;
    # |     set work.grade_month;
    # |     by grade;
    # |
    # |     prev_funded = lag(funded);
    # |     funded_chg  = dif(funded);
    # |
    # |     if first.grade then do;
    # |         prev_funded = .;
    # |         funded_chg  = .;
    # |     end;
    # |
    # |     keep grade issue_d n_loans funded prev_funded funded_chg;
    # | run;
    df = grade_month
    w_grade = Window.partitionBy("grade").orderBy("issue_d")
    df = df.withColumn("prev_funded", F.lag(F.col("funded")).over(w_grade))
    df = df.withColumn("funded_chg", (F.col("funded") - F.lag(F.col("funded")).over(w_grade)))
    grade_mom = df.select("grade", "issue_d", "n_loans", "funded", "prev_funded", "funded_chg")

    return {"grade_month": grade_month, "grade_mom": grade_mom}
