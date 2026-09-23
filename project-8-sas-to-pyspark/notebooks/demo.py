# Databricks notebook source
# MAGIC %md
# MAGIC # SAS → PySpark, converted automatically and proven by parity
# MAGIC
# MAGIC A legacy SAS program goes in; readable PySpark comes out; the PySpark runs on this cluster;
# MAGIC and its output is compared row by row against what the SAS program produces.
# MAGIC
# MAGIC ```
# MAGIC  .sas  ──▶  converter  ──▶  PySpark  ──▶  run on Spark  ──▶  parity check vs SAS answer key  ──▶  PASS / FAIL
# MAGIC ```
# MAGIC
# MAGIC **Before running:** the LendingClub sample (`loans_sample.csv`, 100k loans) must be in a Unity Catalog
# MAGIC Volume — set its path in the `data_path` widget above. Compute: **Serverless**.
# MAGIC Pick a program in the `pattern` widget and run all cells.

# COMMAND ----------

import os
import sys

# This notebook lives in <project>/notebooks; make the project importable.
PROJECT = os.path.abspath("..")
if PROJECT not in sys.path:
    sys.path.insert(0, PROJECT)

from src import demo, load, naive  # noqa: E402

dbutils.widgets.dropdown("pattern", "02", list(demo.pattern_choices()), "pattern")
dbutils.widgets.text("data_path", "/Volumes/workspace/default/raw/loans_sample.csv", "data_path")

PATTERN = dbutils.widgets.get("pattern")
DATA_PATH = dbutils.widgets.get("data_path")
print(f"project : {PROJECT}\npattern : {demo.pattern_choices()[PATTERN]}\ndata    : {DATA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The data
# MAGIC 100,000 LendingClub loans, read with an explicit schema (SAS's PROC IMPORT type-guessing is replaced, not translated).

# COMMAND ----------

loans = load.spark_loans(spark, DATA_PATH)
print(f"{loans.count():,} loans")
display(loans.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The legacy SAS program
# MAGIC Each program's header comment names the trap that makes a naive migration silently wrong.

# COMMAND ----------

displayHTML(demo.sas_html(PATTERN))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Convert it
# MAGIC The converter parses the program, tracks every dataset's columns and sort order, and chooses a translation
# MAGIC per step: **vectorised** (columns and windows), **sequential** (walk each BY group in order) or
# MAGIC **unsupported** (refused with a reason — never guessed). The report says what it did and why.

# COMMAND ----------

conversion = demo.convert_pattern(PATTERN)
print(demo.report_text(conversion))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. SAS next to the generated PySpark

# COMMAND ----------

displayHTML(demo.side_by_side_html(PATTERN, conversion))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run the generated PySpark on Spark

# COMMAND ----------

output = demo.run_converted(PATTERN, conversion, spark, DATA_PATH)
display(output.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Prove it — parity against the SAS answer key
# MAGIC Row counts, per-column checksums, null rates, numeric aggregates, and a row-level diff on the business key.
# MAGIC The answer key is real SAS output when `expected/` has it, otherwise a literal row-by-row Python re-enactment of SAS semantics.

# COMMAND ----------

key_name, report = demo.check(PATTERN, load.to_pandas(output), DATA_PATH)
displayHTML(demo.verdict_html(f"converted PySpark vs {key_name}", report))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Break it — a naive translation
# MAGIC What a quick line-by-line rewrite produces. It runs and the numbers look plausible. The harness shows exactly which rows are wrong.

# COMMAND ----------

if PATTERN in naive.NAIVE:
    key_name, naive_report = demo.check(PATTERN, naive.run(PATTERN, DATA_PATH), DATA_PATH)
    displayHTML(demo.verdict_html(f"naive PySpark vs {key_name}", naive_report))
else:
    print(f"No naive version for pattern {PATTERN}; try 01, 02, 05, 07 or 10.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. It refuses what it can't prove
# MAGIC SAS `LAG()` is a queue that only advances when called. Inside an `IF` it is *not* "the previous row", and a window
# MAGIC function would silently give different numbers — so the converter stops and says why.

# COMMAND ----------

tricky = """
data work.prev_payment;
    set work.loans;
    by grade;
    if int_rate > 15 then prev_rate = lag(int_rate);
run;
"""
refused = demo.convert(tricky, "inline.sas", load.INPUT_SCHEMAS)
print(demo.report_text(refused))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Every program at once

# COMMAND ----------

display(demo.scoreboard(spark, DATA_PATH))
