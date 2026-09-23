"""One place to get a SparkSession, locally or on Databricks."""
from __future__ import annotations

import os
import sys


def get_spark():
    """Return the active session on Databricks, or a small local one for dev/tests.

    Locally, Spark's Python workers default to whatever `python3` is on PATH,
    which isn't the project venv — pandas UDFs then fail with
    ModuleNotFoundError. Pinning PYSPARK_PYTHON to this interpreter fixes that.
    """
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (
        SparkSession.builder.master("local[2]")
        .appName("sas2databricks")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
