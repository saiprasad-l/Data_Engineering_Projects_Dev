# expected/ — real SAS output

CSV files exported from SAS OnDemand by `sas/run_all.sas` (via `sas/99_export.sas`),
one per program output dataset. When a file is present, parity checks the
migrated PySpark against it instead of the Python oracle.

Check the oracle itself against real SAS:

```bash
python -m src.parity --pattern 02 --validate-oracle
```
