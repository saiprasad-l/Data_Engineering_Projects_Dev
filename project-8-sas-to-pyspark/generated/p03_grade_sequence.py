"""PySpark generated from sas/03_grade_sequence.sas by the sas2databricks converter.

Do not edit by hand. Regenerate with:
    python -m src.converter sas/03_grade_sequence.sas

Conversion report
-----------------
[  CONVERTED] PROC SORT work.loans -> work.loans_by_grade (lines 16-18)
              - PROC SORT dropped: Spark DataFrames have no row order. Sort key (grade, issue_d, id) kept as metadata and used to order windows downstream.
[  CONVERTED] DATA work.grade_seq (lines 20-31)
              - `loan_seq + 1` (sum statement, implied RETAIN, reset on FIRST.grade) -> row_number()
"""
# ruff: noqa
import math
from datetime import date

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import sasrt as sas

INPUTS = ['loans']
OUTPUTS = ['loans_by_grade', 'grade_seq']


def run(spark: SparkSession, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    loans = tables["loans"]

    # ──── PROC SORT work.loans -> work.loans_by_grade  (sas lines 16-18) ────
    # | proc sort data=work.loans out=work.loans_by_grade;
    # |     by grade issue_d id;
    # | run;
    loans_by_grade = loans  # sort key carried forward: ['grade', 'issue_d', 'id']

    # ──── DATA work.grade_seq  (sas lines 20-31) ────
    # | data work.grade_seq;
    # |     set work.loans_by_grade;
    # |     by grade;
    # |
    # |     if first.grade then loan_seq = 0;
    # |     loan_seq + 1;
    # |
    # |     is_first = first.grade;
    # |     is_last  = last.grade;
    # |
    # |     keep grade issue_d id loan_seq is_first is_last;
    # | run;
    df = loans_by_grade
    w_grade = Window.partitionBy("grade").orderBy("issue_d", "id")
    df = df.withColumn("_rn_grade", F.row_number().over(w_grade))
    df = df.withColumn("_n_grade", F.count(F.lit(1)).over(Window.partitionBy("grade")))
    df = df.withColumn("loan_seq", F.row_number().over(w_grade))
    df = df.withColumn("is_first", (F.col("_rn_grade") == 1).cast('int'))
    df = df.withColumn("is_last", (F.col("_rn_grade") == F.col("_n_grade")).cast('int'))
    grade_seq = df.select("grade", "issue_d", "id", "loan_seq", "is_first", "is_last")

    return {"loans_by_grade": loans_by_grade, "grade_seq": grade_seq}
