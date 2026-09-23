"""Converter: parsing, translation choices, SAS rules in the runtime, and
end-to-end parity on the real sample."""
import math

import pytest

from src import legacy, load, naive, parity, sasrt
from src.converter import convert, ir, parse

SCHEMA = {"t": {"k": "string", "d": "date", "x": "double", "y": "double", "s": "string"}}


def code_for(sas: str) -> str:
    return convert(sas, "test.sas", SCHEMA).code


def report_for(sas: str):
    return convert(sas, "test.sas", SCHEMA).reports


# -- parser ------------------------------------------------------------------

def test_parser_builds_if_else_chains_and_blocks():
    prog = parse("""
        data out; set t;
          /* comment ; with a semicolon */
          if x >= 1 and y < 2 then z = 'A';
          else if x = . then z = 'B';
          else do; z = 'C'; w = 1; end;
          n + 1;
        run;""")
    [step] = prog.steps
    s = [x for x in step.stmts if isinstance(x, ir.If)][0]
    assert s.cond == ir.Binary("and", ir.Binary(">=", ir.Var("x"), ir.Num(1)), ir.Binary("<", ir.Var("y"), ir.Num(2)))
    assert isinstance(s.else_[0], ir.If) and len(s.else_[0].else_) == 2
    assert any(isinstance(x, ir.SumStmt) for x in step.stmts)


def test_parser_reads_date_literals_and_dataset_options():
    prog = parse("data o; set t(where=(d >= '01OCT2018'd) rename=(x=x2) keep=k d x); run;")
    ref = prog.steps[0].stmts[0].datasets[0]
    assert ref.options.where.right == ir.Date(__import__("datetime").date(2018, 10, 1))
    assert ref.options.rename == {"x": "x2"} and ref.options.keep == ["k", "d", "x"]


# -- translation choices --------------------------------------------------------

def test_missing_is_smaller_than_any_number():
    code = code_for("data o; set t; if x < 5 then f = 1; else f = 0; run;")
    assert 'F.col("x").isNull() | (F.col("x") < 5)' in code


def test_running_total_uses_rows_frame_and_full_sort_key():
    code = code_for("""
        proc sort data=t out=s; by k d; run;
        data o; set s; by k; retain c 0; if first.k then c = 0; c = c + x; run;""")
    assert 'Window.partitionBy("k").orderBy("d").rowsBetween(Window.unboundedPreceding, Window.currentRow)' in code


def test_self_referential_retain_goes_sequential():
    [_, rep] = report_for("""
        proc sort data=t out=s; by k d; run;
        data o; set s; by k; retain b; if first.k then b = x; b = round(b * 1.01 - y, 0.01); run;""")
    assert rep.status == "sequential"


def test_conditional_lag_is_refused_not_guessed():
    result = convert("data o; set t; by k; if x > 0 then p = lag(x); run;", "t.sas", SCHEMA)
    assert not result.ok
    assert "queue" in result.reports[0].notes[0]
    assert "raise NotImplementedError" in result.code


def test_unknown_proc_is_reported():
    result = convert("proc transpose data=t out=o; run;", "t.sas", SCHEMA)
    assert result.reports[0].status == "unsupported"


def test_first_assignment_fixes_character_length():
    notes = report_for("data o; set t; if x > 1 then r = 'LOW'; else r = 'MEDIUM'; run;")[0].notes
    assert any("'MEDIUM' is truncated to 'MED'" in n for n in notes)


def test_generated_files_are_up_to_date():
    for pid in load.PATTERNS:
        committed = load.generated_path(pid).read_text()
        fresh = convert(load.sas_program(pid), f"sas/{load.PATTERNS[pid].sas_file}", load.INPUT_SCHEMAS).code
        assert committed == fresh, f"generated/{load.generated_path(pid).name} is stale: python -m src.converter --all"


# -- runtime SAS rules -----------------------------------------------------------

def test_runtime_round_matches_sas_in_python_and_spark(spark):
    values = [2.675, 0.125, -0.125, 1.004, 54.884999999999998]
    expect = [legacy.sas_round(v, 0.01) for v in values]
    assert [sasrt.py_round(v, 0.01) for v in values] == expect
    df = spark.createDataFrame([(v,) for v in values], "v double")
    got = [r[0] for r in df.select(sasrt.round_(df.v, 0.01)).collect()]
    assert got == expect


def test_runtime_do_to_runs_zero_times_when_stop_below_start(spark):
    from pyspark.sql import functions as F
    df = spark.createDataFrame([(0.0,), (3.0,)], "stop double")
    rows = df.select(F.explode(sasrt.do_to(1, F.col("stop"))).alias("i")).collect()
    assert [r.i for r in rows] == [1, 2, 3]


def test_runtime_comparisons_never_return_null(spark):
    df = spark.createDataFrame([(None, 1.0), (1.0, None), (None, None)], "a double, b double")
    got = [tuple(r) for r in df.select(sasrt.lt(df.a, df.b),
                                       sasrt.ge(df.a, df.b)).collect()]
    assert got == [(True, False), (False, True), (False, True)]
    assert sasrt.py_lt(math.nan, 1) and not sasrt.py_lt(1, math.nan)


# -- end to end ------------------------------------------------------------------

@pytest.mark.parametrize("pattern", list(load.PATTERNS))
def test_converted_program_matches_sas_semantics(pattern, loans):
    report = parity.compare(legacy.run(pattern, loans), load.migrated(pattern), key=load.KEYS[pattern])
    assert report.passed, str(report)


@pytest.mark.parametrize("pattern", list(naive.NAIVE))
def test_naive_translation_is_caught(pattern, loans):
    report = parity.compare(legacy.run(pattern, loans), naive.run(pattern), key=load.KEYS[pattern])
    assert not report.passed
