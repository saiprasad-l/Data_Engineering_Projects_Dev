"""Load the sample and describe the migration patterns.

Contract used by parity.py:
    loans()                -> pandas DataFrame of the sample, SAS-style types
    migrated(pattern, df)  -> output of the converter-generated PySpark
    KEYS                   -> business key per pattern, for keyed diffs

Answer keys:
    expected(pattern)      -> real SAS output from expected/ (SAS OnDemand), if present
    legacy.run(pattern,df) -> the Python oracle, always available
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "data" / "sample" / "loans_sample.csv"
SAS_DIR = ROOT / "sas"
EXPECTED_DIR = ROOT / "expected"

# Character columns. Everything else is numeric (SAS has only the two types).
# issue_d stays an ISO string here: it sorts and compares correctly as text,
# and matches how Spark's DateType prints, so checksums line up.
CHAR_COLUMNS = [
    "grade", "sub_grade", "emp_length", "home_ownership", "verification_status",
    "issue_d", "loan_status", "purpose", "addr_state",
]


@dataclass(frozen=True)
class Pattern:
    sas_file: str       # program in sas/
    output: str         # WORK dataset the program produces
    key: list[str]      # business key of that output


PATTERNS: dict[str, Pattern] = {
    "01": Pattern("01_risk_tier.sas",            "loan_risk",         ["id"]),
    "02": Pattern("02_state_running_total.sas",  "state_running",     ["id"]),
    "03": Pattern("03_grade_sequence.sas",       "grade_seq",         ["id"]),
    "04": Pattern("04_grade_month_change.sas",   "grade_mom",         ["grade", "issue_d"]),
    "05": Pattern("05_portfolio_summary.sas",    "portfolio_summary", ["grade", "emp_length"]),
    "07": Pattern("07_branch_region.sas",        "loans_region",      ["id"]),
    "10": Pattern("10_amortization.sas",         "amortization",      ["id", "month_num"]),
}
KEYS = {pid: p.key for pid, p in PATTERNS.items()}

GENERATED_DIR = ROOT / "generated"

# Spark schema of the sample - explicit, never inferred. SAS numerics are all
# doubles; id is a whole-number key.
SPARK_SCHEMA = {
    "id": "bigint", "loan_amnt": "double", "funded_amnt": "double", "term": "double",
    "int_rate": "double", "installment": "double", "grade": "string", "sub_grade": "string",
    "emp_length": "string", "home_ownership": "string", "annual_inc": "double",
    "verification_status": "string", "issue_d": "date", "loan_status": "string",
    "purpose": "string", "addr_state": "string", "dti": "double", "fico_range_low": "double",
    "fico_range_high": "double", "delinq_2yrs": "double", "revol_util": "double",
    "total_pymnt": "double",
}
# Datasets a SAS program may read without creating (WORK.LOANS comes from 00_import.sas)
INPUT_SCHEMAS = {"loans": SPARK_SCHEMA}


def loans(path: Path = SAMPLE) -> pd.DataFrame:
    """The sample as pandas. Numeric missing -> NaN, character missing -> NaN."""
    if not path.exists():
        raise FileNotFoundError(f"{path} not found - run: python -m src.sample --input <kaggle csv>")
    return pd.read_csv(path, dtype={c: "object" for c in CHAR_COLUMNS})


def sas_program(pattern: str) -> str:
    return (SAS_DIR / PATTERNS[pattern].sas_file).read_text()


def has_expected(pattern: str) -> bool:
    return (EXPECTED_DIR / f"{PATTERNS[pattern].output}.csv").exists()


def expected(pattern: str) -> pd.DataFrame:
    """Real SAS output exported by sas/99_export.sas.

    Normalised to match the oracle's representation: lower-case column names
    (SAS may keep whatever case a variable was created with) and blank
    character values as missing (SAS stores a character missing as blanks).
    """
    path = EXPECTED_DIR / f"{PATTERNS[pattern].output}.csv"
    char = set(CHAR_COLUMNS + ["region", "risk_tier"])
    header = pd.read_csv(path, nrows=0).columns
    df = pd.read_csv(path, dtype={c: "object" for c in header if c.strip().lower() in char},
                     keep_default_na=False, na_values=[""])
    df.columns = [c.strip().lower() for c in df.columns]
    for c in df.select_dtypes("object"):
        df[c] = df[c].str.strip().replace("", pd.NA)
    return df


def spark_loans(spark, path: str | Path = SAMPLE):
    """The sample as a Spark DataFrame. On Databricks pass the Volume path."""
    ddl = ", ".join(f"{c} {t}" for c, t in SPARK_SCHEMA.items())
    return spark.read.csv(str(path), header=True, schema=ddl, dateFormat="yyyy-MM-dd")


def generated_path(pattern: str) -> Path:
    return GENERATED_DIR / f"p{Path(PATTERNS[pattern].sas_file).stem}.py"


def convert(pattern: str):
    """Run the converter on the pattern's SAS program and write generated/pNN_*.py."""
    from .converter import convert as _convert

    result = _convert(sas_program(pattern), f"sas/{PATTERNS[pattern].sas_file}", INPUT_SCHEMAS)
    GENERATED_DIR.mkdir(exist_ok=True)
    generated_path(pattern).write_text(result.code)
    return result


def generated_module(pattern: str):
    import importlib.util

    path = generated_path(pattern)
    spec = importlib.util.spec_from_file_location(f"generated.{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def migrated(pattern: str, df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Convert the SAS program, run the generated PySpark on the sample, return
    the program's output as pandas (dates as ISO strings, like the oracle).

    `df` is accepted for the parity contract but unused: Spark reads the same
    sample file itself, as it would on Databricks."""
    from .spark import get_spark

    convert(pattern)
    spark = get_spark()
    out = generated_module(pattern).run(spark, {"loans": spark_loans(spark)})[PATTERNS[pattern].output]
    date_cols = [f.name for f in out.schema.fields if f.dataType.typeName() == "date"]
    pdf = out.toPandas()
    for c in date_cols:
        pdf[c] = pdf[c].map(lambda d: None if d is None else d.isoformat())
    return pdf
