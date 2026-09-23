"""Naive translations: what a quick line-by-line SAS -> PySpark rewrite produces.

Each one runs, returns plausible numbers, and is wrong. They exist to show the
parity harness catching exactly the traps the converter handles - the "break
it" half of the demo:

    python -m src.parity --pattern 02 --naive
"""
from __future__ import annotations

import math

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def p01(loans: DataFrame) -> DataFrame:
    # Plain Spark comparisons: `dti < 20` is NULL when dti is missing, so the
    # loan falls through to HIGH. SAS says missing < 20 is TRUE -> LOW.
    return loans.select(
        "id", "grade", "fico_range_low", "dti", "revol_util", "loan_status",
        F.when((F.col("fico_range_low") >= 740) & (F.col("dti") < 20), "LOW")
         .when((F.col("fico_range_low") >= 680) & (F.col("dti") < 35), "MEDIUM")
         .otherwise("HIGH").alias("risk_tier"),
        F.when(F.col("revol_util") > 90, 1).otherwise(0).alias("high_util"),
        F.col("loan_status").isin("Charged Off", "Default").cast("int").alias("is_bad"),
    )


def p02(loans: DataFrame) -> DataFrame:
    # Window ordered by issue_d only, default frame. With ORDER BY that default
    # is RANGE: every loan issued in the same month gets the month-end total.
    w = Window.partitionBy("addr_state").orderBy("issue_d")
    return loans.select(
        "addr_state", "issue_d", "id", "funded_amnt",
        F.sum("funded_amnt").over(w).alias("cum_funded"),
        F.count(F.lit(1)).over(w).alias("cum_loans"),
    )


def p05(loans: DataFrame) -> DataFrame:
    # groupBy keeps a NULL emp_length group (PROC MEANS drops it) and
    # count("*") is used for N(dti) (N counts non-missing values only).
    return loans.groupBy("grade", "emp_length").agg(
        F.count("*").alias("n_loans"),
        F.sum("funded_amnt").alias("total_funded"),
        F.avg("int_rate").alias("avg_rate"),
        F.min("int_rate").alias("min_rate"),
        F.max("int_rate").alias("max_rate"),
        F.count("*").alias("n_dti"),
        F.avg("dti").alias("avg_dti"),
    )


def p07(loans: DataFrame, spark) -> DataFrame:
    # "MERGE is a join": an inner join silently drops loans in states with no
    # branch region instead of keeping them as UNMAPPED.
    from . import legacy
    regions = spark.createDataFrame(sorted(legacy.BRANCH_REGION.items()), "addr_state string, region string")
    return loans.join(regions, "addr_state", "inner").select("id", "addr_state", "region", "funded_amnt")


def p10(loans: DataFrame) -> DataFrame:
    # The loop itself is right, but uses Python's round(): banker's rounding
    # (0.125 -> 0.12) where SAS rounds halves away from zero (0.13).
    sched = (loans.where((F.col("issue_d") >= "2018-10-01") & (F.col("grade") == "A"))
             .withColumn("month_num", F.explode(F.sequence(F.lit(1), F.col("term").cast("int"))))
             .withColumn("monthly_rate", F.col("int_rate") / 100 / 12))

    def walk(pdf: pd.DataFrame) -> pd.DataFrame:
        out, balance = [], math.nan
        for i, r in enumerate(pdf.sort_values("month_num").to_dict("records")):
            if i == 0:
                balance = r["funded_amnt"]
            interest = round(balance * r["monthly_rate"], 2)
            principal = round(r["installment"] - interest, 2)
            balance = max(0.0, round(balance - principal, 2))
            out.append((r["id"], r["month_num"], interest, principal, balance))
        return pd.DataFrame(out, columns=["id", "month_num", "interest", "principal", "balance"])

    return sched.groupBy("id").applyInPandas(
        walk, "id bigint, month_num int, interest double, principal double, balance double")


NAIVE = {"01": p01, "02": p02, "05": p05, "07": p07, "10": p10}


def run(pattern: str, data_path=None) -> pd.DataFrame:
    from . import load
    from .spark import get_spark

    spark = get_spark()
    loans = load.spark_loans(spark, data_path or load.SAMPLE)
    fn = NAIVE[pattern]
    out = fn(loans, spark) if pattern == "07" else fn(loans)
    return load.to_pandas(out)
