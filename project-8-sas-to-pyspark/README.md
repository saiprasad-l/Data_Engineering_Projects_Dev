# sas2databricks

Migrating legacy SAS analytics code to Databricks (Spark SQL + PySpark), with a
parity harness that **proves** the migration is correct instead of assuming it.

Built on public [LendingClub](https://www.kaggle.com/datasets/wordsforthewise/lending-club)
consumer-loan data. No proprietary code or data.

## Why

SAS has been the default in credit-risk and collections analytics for decades, and
nearly every lender is now moving that code onto a lakehouse. The hard part isn't
syntax — it's that SAS has two halves that translate very differently:

- **`PROC SQL`** → nearly 1:1 with Spark SQL. Easy.
- **The DATA step** → a *row-by-row loop with memory*. `RETAIN` carries state
  across rows, `BY` groups expose `FIRST.`/`LAST.`, execution is sequential.
  Spark SQL is set-based and has no loop.

Bridging the second one is the whole job, and it's where migrations silently
change results. This repo catalogs the patterns and then verifies parity.

## Pattern catalog

| SAS | Databricks SQL | File |
|---|---|---|
| `IF/THEN/ELSE` | `CASE WHEN` | [`sql/01_conditional.sql`](sql/01_conditional.sql) |
| `RETAIN` running total | `SUM(x) OVER (PARTITION BY .. ORDER BY .. ROWS UNBOUNDED PRECEDING)` | [`sql/02_retain.sql`](sql/02_retain.sql) |
| `BY` group `FIRST.` / `LAST.` | `ROW_NUMBER() OVER (..) = 1` / `= COUNT(*) OVER (..)` | [`sql/03_first_last.sql`](sql/03_first_last.sql) |
| `LAG()` / `DIF()` | `LAG(x) OVER (..)`, `x - LAG(x) OVER (..)` | [`sql/04_lag_dif.sql`](sql/04_lag_dif.sql) |
| `PROC MEANS` / `SUMMARY` | `GROUP BY` + aggregates | [`sql/05_proc_means.sql`](sql/05_proc_means.sql) |
| `PROC TRANSPOSE` | `PIVOT` / `UNPIVOT` | [`sql/06_transpose.sql`](sql/06_transpose.sql) |
| `MERGE .. BY` | `MERGE INTO` (Delta) / `FULL OUTER JOIN` | [`sql/07_merge.sql`](sql/07_merge.sql) |
| `PROC FORMAT` | `CASE` or a joined mapping table | [`sql/08_formats.sql`](sql/08_formats.sql) |
| `%LET` / `%MACRO` | widgets, SQL variables, SQL UDFs | [`sql/09_macros.sql`](sql/09_macros.sql) |
| Self-referential `RETAIN` | recursive CTE, or fall back to PySpark | [`sql/10_sequential.sql`](sql/10_sequential.sql) |

### Where SQL genuinely breaks down

Pattern 10 is the honest edge case: when row *N* depends on a **computed** value
from row *N−1* (not just a lagged input), no single window function expresses it.
Options are a recursive CTE, or dropping to PySpark. Knowing which cases need
that is most of the skill.

## Parity harness

`src/parity.py` compares legacy output against migrated output and reports:

- row count delta
- column-level checksums (hash of sorted values per column)
- null-rate per column
- aggregate reconciliation (sum/min/max/mean on numerics)
- a sample diff of mismatched keys

A migration isn't done when it runs — it's done when this is clean.

## Layout

```
sql/          SAS construct -> Spark SQL, one file per pattern
src/
  load.py     LendingClub loader + schema
  legacy.py   pandas reference implementing SAS semantics
  parity.py   the comparison harness
notebooks/    Databricks notebooks tying it together
```

## Run

```bash
pip install -r requirements.txt
python -m src.load --sample 100000      # download + stage
python -m src.parity --pattern 02       # legacy vs migrated
```

On Databricks: import `notebooks/`, attach to a job cluster, run
`00_run_all`. Cluster config and runtimes are in the notebook header.

## Status

Pattern catalog and parity harness first. Runtime benchmarking (job cluster
sizing, Z-ORDER, broadcast joins) to follow.
