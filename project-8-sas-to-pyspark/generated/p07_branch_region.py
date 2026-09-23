"""PySpark generated from sas/07_branch_region.sas by the sas2databricks converter.

Do not edit by hand. Regenerate with:
    python -m src.converter sas/07_branch_region.sas

Conversion report
-----------------
[  CONVERTED] DATA work.branch_region (lines 20-65)
              - DATALINES (40 rows) -> spark.createDataFrame
[  CONVERTED] PROC SORT work.loans -> work.loans_by_state (lines 67-69)
              - PROC SORT dropped: Spark DataFrames have no row order. Sort key (addr_state) kept as metadata and used to order windows downstream.
[  CONVERTED] PROC SORT work.branch_region (lines 71-73)
              - PROC SORT dropped: Spark DataFrames have no row order. Sort key (addr_state) kept as metadata and used to order windows downstream.
[  CONVERTED] DATA work.loans_region (lines 75-84)
              - MERGE BY addr_state: `if in_loans;` -> left join; runtime guard stops if both sides repeat a BY value (SAS pairs rows, a join would multiply them)
"""
# ruff: noqa
import math
from datetime import date

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import sasrt as sas

INPUTS = ['loans']
OUTPUTS = ['branch_region', 'loans_by_state', 'loans_region']


def run(spark: SparkSession, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    loans = tables["loans"]

    # ──── DATA work.branch_region  (sas lines 20-65) ────
    # | data work.branch_region;
    # |     length addr_state $2 region $12;
    # |     input addr_state $ region $;
    # |     datalines;
    # |     ... (in-stream data, see createDataFrame below)
    # | ;
    # | run;
    branch_region = spark.createDataFrame(
        [
            ('CT', 'Northeast'),
            ('MA', 'Northeast'),
            ('NJ', 'Northeast'),
            ('NY', 'Northeast'),
            ('PA', 'Northeast'),
            ('RI', 'Northeast'),
            ('DE', 'MidAtlantic'),
            ('MD', 'MidAtlantic'),
            ('VA', 'MidAtlantic'),
            ('WV', 'MidAtlantic'),
            ('NC', 'Southeast'),
            ('SC', 'Southeast'),
            ('GA', 'Southeast'),
            ('FL', 'Southeast'),
            ('AL', 'Southeast'),
            ('TN', 'Southeast'),
            ('KY', 'Southeast'),
            ('MS', 'Southeast'),
            ('IL', 'Midwest'),
            ('IN', 'Midwest'),
            ('IA', 'Midwest'),
            ('MI', 'Midwest'),
            ('MN', 'Midwest'),
            ('MO', 'Midwest'),
            ('OH', 'Midwest'),
            ('WI', 'Midwest'),
            ('KS', 'Midwest'),
            ('NE', 'Midwest'),
            ('AR', 'SouthCentral'),
            ('LA', 'SouthCentral'),
            ('OK', 'SouthCentral'),
            ('TX', 'SouthCentral'),
            ('AZ', 'West'),
            ('CA', 'West'),
            ('CO', 'West'),
            ('NV', 'West'),
            ('NM', 'West'),
            ('OR', 'West'),
            ('UT', 'West'),
            ('WA', 'West'),
        ],
        "addr_state string, region string",
    )

    # ──── PROC SORT work.loans -> work.loans_by_state  (sas lines 67-69) ────
    # | proc sort data=work.loans out=work.loans_by_state;
    # |     by addr_state;
    # | run;
    loans_by_state = loans  # sort key carried forward: ['addr_state']

    # ──── PROC SORT work.branch_region  (sas lines 71-73) ────
    # | proc sort data=work.branch_region;
    # |     by addr_state;
    # | run;
    # branch_region: sort key carried forward: ['addr_state']

    # ──── DATA work.loans_region  (sas lines 75-84) ────
    # | data work.loans_region;
    # |     merge work.loans_by_state(in=in_loans)
    # |           work.branch_region(in=in_ref);
    # |     by addr_state;
    # |
    # |     if in_loans;
    # |     if not in_ref then region = 'UNMAPPED';
    # |
    # |     keep id addr_state region funded_amnt;
    # | run;
    sas.assert_no_many_to_many(loans_by_state, branch_region, ["addr_state"], "DATA work.loans_region")
    df = (
        loans_by_state.withColumn("in_loans", F.lit(True))
        .join(branch_region.withColumn("in_ref", F.lit(True)), on=["addr_state"], how="left")
        .withColumn("in_loans", F.coalesce(F.col("in_loans"), F.lit(False)))
        .withColumn("in_ref", F.coalesce(F.col("in_ref"), F.lit(False)))
    )
    df = df.withColumn("region", (
        F.when(~F.col("in_ref"), 'UNMAPPED')
        .otherwise(F.col("region"))
    ))
    loans_region = df.select("id", "addr_state", "region", "funded_amnt")

    return {"branch_region": branch_region, "loans_by_state": loans_by_state, "loans_region": loans_region}
