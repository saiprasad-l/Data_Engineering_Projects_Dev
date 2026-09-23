# SAS → PySpark converter, with parity proof

A converter that reads legacy SAS programs and writes readable PySpark, plus a
parity harness that **proves** the PySpark produces the same numbers as the SAS
program, row by row, instead of assuming it.

Built on public [LendingClub](https://www.kaggle.com/datasets/wordsforthewise/lending-club)
loan data (100,000-loan sample). No proprietary code or data.

```
 .sas  ──▶  parser  ──▶  generator  ──▶  PySpark  ──▶  run on Spark  ──▶  parity vs SAS answer key  ──▶  PASS / FAIL
```

## Results

Seven credit-risk SAS programs, converted with no hand edits. Each is then
checked against the SAS answer key. A naive line-by-line translation of each
runs fine, looks plausible, and is wrong. The harness catches every one.

| SAS program | Converted PySpark | Naive translation | What the naive version gets wrong |
|---|---|---|---|
| `01_risk_tier` | ✅ PASS | ❌ FAIL | 56 loans with missing DTI in the wrong risk tier |
| `02_state_running_total` | ✅ PASS | ❌ FAIL | 95,420 running totals wrong: same-month loans collapse into one total |
| `03_grade_sequence` | ✅ PASS | — | |
| `04_grade_month_change` | ✅ PASS | — | |
| `05_portfolio_summary` | ✅ PASS | ❌ FAIL | 7 extra NULL groups, wrong non-missing counts |
| `07_branch_region` | ✅ PASS | ❌ FAIL | 2,982 loans silently dropped by an inner join |
| `10_amortization` | ✅ PASS | ❌ FAIL | 2,072 payment rows off: one cent of rounding compounds through the schedule |

## Why this is hard

SAS has two halves that migrate very differently. `PROC SQL` maps almost 1:1.
The **DATA step** doesn't: it is a row-by-row loop with memory (`RETAIN`,
`FIRST.`/`LAST.`, `LAG`) that depends on physical row order. Spark is
set-based and has no row order. Most migration bugs live in that gap, and they
don't crash. They change the numbers.

## The patterns and their traps

| # | SAS construct | Converted to | The trap |
|---|---|---|---|
| 01 | `IF / THEN / ELSE` | `F.when` chain | SAS treats missing as **smaller than every number**, so `dti < 20` is TRUE for a missing DTI. In Spark it is NULL. |
| 02 | `RETAIN` running total within `BY` | `F.sum` over a **ROWS** window ordered by the full sort key | Ordered by month only, Spark's default RANGE frame gives every same-month row the month-end total |
| 03 | `FIRST.` / `LAST.`, sum statement `n + 1` | `row_number()`, `row_number() == count()` | `n + 1;` silently implies RETAIN |
| 04 | `PROC SUMMARY` → `LAG` / `DIF` | `groupBy` → `F.lag` over a partitioned window | SAS `LAG` is a queue, not "the previous row". Called conditionally, it gives different answers. |
| 05 | `PROC MEANS NWAY` | `where(class not null).groupBy().agg()` | PROC MEANS **drops** missing CLASS values; `N(x)` counts non-missing, `_FREQ_` counts rows |
| 07 | `MERGE ... BY` with `IN=` | `join` chosen from the `IN=` logic + a many-to-many guard | MERGE is not a join: it pairs rows within a BY group, and the last dataset wins on shared columns |
| 10 | `DO` loop `OUTPUT` + self-referential `RETAIN` | `explode(sequence)` + `groupBy().applyInPandas` | This month's balance depends on last month's **computed** balance. No window function can express it. SAS `ROUND` also differs from Python's and Spark's. |

## How the converter works

- **Parser** ([src/converter/parser.py](src/converter/parser.py)): reads DATA
  steps (SET, MERGE, BY, RETAIN, IF/ELSE, DO, sum statements, LAG/DIF,
  DATALINES), PROC SORT and PROC MEANS/SUMMARY into a small syntax tree.
- **Generator** ([src/converter/generate.py](src/converter/generate.py)):
  tracks every dataset's columns, types and **sort order** through the program.
  PROC SORT becomes metadata, and the sort key becomes explicit window ordering
  downstream. Each DATA step gets one of three translations:
  - **vectorised**: columns and window functions, when the step's state can be
    expressed exactly that way (running totals, FIRST./LAST., LAG)
  - **sequential**: `groupBy(BY).applyInPandas`, walking rows in order, when a
    value depends on its own previous computed value
  - **unsupported**: the step raises `NotImplementedError` and the report says
    why. The converter never guesses.
- **SAS runtime** ([src/sasrt.py](src/sasrt.py)): the few SAS rules Spark
  doesn't have (ROUND half-away-from-zero, missing-aware comparisons, DO-loop
  ranges, the MERGE guard).
- **Output**: [generated/](generated/) holds one module per program. Each one
  shows the original SAS above every step and a report of each translation
  decision.

## The proof

[src/parity.py](src/parity.py) compares the migrated output with the answer
key: row counts, per-column checksums (order- and type-independent), null
rates, numeric aggregates, and a **row-level diff on the business key**
showing exactly which rows and columns differ.

The answer key is either:
- **real SAS output**, exported from SAS OnDemand into [expected/](expected/), used automatically when present, or
- [src/legacy.py](src/legacy.py), a deliberately literal row-by-row Python
  re-enactment of SAS semantics. It shares no code with the converter or its
  runtime, so the two can't be wrong in the same way.

## Run it locally

Needs Python 3.10+ and Java 17+.

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
```

```bash
.venv/bin/python -m src.sample --input path/to/accepted_2007_to_2018Q4.csv.gz
```

```bash
.venv/bin/python -m src.converter --all
```

```bash
.venv/bin/python -m src.parity --pattern 02
```

```bash
.venv/bin/python -m src.parity --pattern 02 --naive
```

```bash
.venv/bin/python -m pytest
```

In order: set up the environment, build the 100k-loan sample, convert every
SAS program to `generated/`, check one conversion (PASS), check the naive
translation (FAIL), and run all the tests.

## Run the demo on Databricks

Works on Databricks Free Edition (serverless).

1. **Upload the data:** Catalog → `workspace` → `default` → Create → Volume
   `raw`, then upload `data/sample/loans_sample.csv`. The path becomes
   `/Volumes/workspace/default/raw/loans_sample.csv`.
2. **Add the code:** Workspace → Create → Git folder → this repository.
3. **Open** [notebooks/demo.py](notebooks/demo.py), attach **Serverless**, pick
   a program in the `pattern` widget and **Run all**.

The notebook shows the SAS program, converts it live, puts SAS and PySpark side
by side, runs the PySpark, proves parity (PASS), breaks it with a naive
translation (FAIL, showing the wrong rows), shows the converter refusing a
construct it can't translate safely, and finishes with a scoreboard of all
seven programs.

## Checking the answer key against real SAS

1. In SAS OnDemand for Academics (SAS Studio), upload
   `data/sample/loans_sample.csv` to `~/sas2databricks/data/` and the `sas/`
   folder to `~/sas2databricks/sas/`.
2. Run `sas/run_all.sas`. It runs every program and writes
   `~/sas2databricks/expected/*.csv` through [sas/99_export.sas](sas/99_export.sas).
3. Download the CSVs into [expected/](expected/). Parity then uses real SAS as
   the answer key.

To check the Python answer key itself against SAS:

```bash
.venv/bin/python -m src.parity --pattern 10 --validate-oracle
```

## Project layout

```
sas/            legacy SAS programs (converter input) + run_all / export for SAS OnDemand
src/
  converter/    parser, expression translator, generator, CLI
  sasrt.py      SAS runtime helpers used by generated code
  legacy.py     the answer key: literal re-enactment of SAS semantics
  parity.py     the comparison harness
  naive.py      naive translations the harness must catch
  load.py       data loading, schemas, pattern registry
  sample.py     builds the 100k-loan sample from the Kaggle file
  demo.py       helpers behind the Databricks notebook
generated/      converter output, one PySpark module per SAS program
notebooks/      Databricks demo notebook
expected/       real SAS output (optional)
tests/          parser, converter, runtime rules, end-to-end parity
```

## Scope and limits

The converter covers the constructs above and refuses the rest with a reason:
`PROC SQL`, `PROC TRANSPOSE`, `PROC FORMAT`, macros (`%LET`, `%MACRO`), arrays,
`DO WHILE/UNTIL`, conditional `LAG`, `PROC MEANS` without `NWAY`, and MERGE of
more than two datasets. Adding a construct means a parser rule, a generator
rule, a SAS program that uses it, and its entry in the answer key. Parity then
says whether the translation is right.
