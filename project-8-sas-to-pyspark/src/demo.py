"""Helpers behind notebooks/demo.py, kept here so they can be run and tested
outside Databricks."""
from __future__ import annotations

import html
from functools import lru_cache
from pathlib import Path

import pandas as pd

from . import legacy, load, naive, parity
from .converter import Conversion, convert

TRANSLATION = {"converted": "vectorised", "sequential": "sequential (applyInPandas)"}


def pattern_choices() -> dict[str, str]:
    return {pid: f"{pid}  {Path(p.sas_file).stem[3:]}" for pid, p in load.PATTERNS.items()}


def convert_pattern(pattern: str) -> Conversion:
    return convert(load.sas_program(pattern), f"sas/{load.PATTERNS[pattern].sas_file}", load.INPUT_SCHEMAS)


def run_converted(pattern: str, conversion: Conversion, spark, data_path):
    """Execute the generated module in memory; return the program's output DataFrame."""
    module = load.module_from_code(conversion.code, f"generated_p{pattern}")
    return module.run(spark, {"loans": load.spark_loans(spark, data_path)})[load.PATTERNS[pattern].output]


@lru_cache(maxsize=4)
def _loans(data_path: str) -> pd.DataFrame:
    return load.loans(Path(data_path))


def answer_key(pattern: str, data_path) -> tuple[str, pd.DataFrame]:
    """Real SAS output when expected/ has it, otherwise the Python oracle."""
    if load.has_expected(pattern):
        return "real SAS output", load.expected(pattern)
    return "Python oracle (SAS semantics)", legacy.run(pattern, _loans(str(data_path)))


def report_text(conversion: Conversion) -> str:
    out = []
    for r in conversion.reports:
        mark = "✘" if r.status == "unsupported" else "✔"
        out.append(f"{mark} {r.title}  [{TRANSLATION.get(r.status, r.status)}]")
        out += [f"    - {n}" for n in r.notes]
    return "\n".join(out)


_STYLE = """
<style>
 .s2p {font-family: -apple-system, Segoe UI, Roboto, sans-serif; color: #1f2328;}
 .s2p .cols {display: flex; gap: 12px; align-items: stretch;}
 .s2p .col {flex: 1; min-width: 0; display: flex; flex-direction: column;}
 .s2p h4 {margin: 0 0 6px; font-size: 13px; letter-spacing: .04em; text-transform: uppercase; color: #59636e;}
 .s2p pre {margin: 0; flex: 1; padding: 12px; border-radius: 6px; font-size: 12px; line-height: 1.45;
           overflow-x: auto; background: #f6f8fa; border: 1px solid #d1d9e0; color: #1f2328;}
 .s2p .verdict {padding: 14px 16px; border-radius: 6px; font-size: 18px; font-weight: 600; margin-bottom: 8px;}
 .s2p .pass {background: #dafbe1; color: #1a7f37; border: 1px solid #4ac26b;}
 .s2p .fail {background: #ffebe9; color: #d1242f; border: 1px solid #ff8182;}
</style>
"""


def _pre(text: str) -> str:
    return f"<pre>{html.escape(text)}</pre>"


def sas_html(pattern: str) -> str:
    return _STYLE + f'<div class="s2p"><h4>sas/{load.PATTERNS[pattern].sas_file}</h4>{_pre(load.sas_program(pattern))}</div>'


def side_by_side_html(pattern: str, conversion: Conversion) -> str:
    """SAS program next to the generated run() body (without the repeated SAS comments)."""
    body = conversion.code[conversion.code.index("def run("):]
    body = "\n".join(ln for ln in body.splitlines() if not ln.lstrip().startswith("# |"))
    return _STYLE + f"""<div class="s2p"><div class="cols">
      <div class="col"><h4>Legacy SAS</h4>{_pre(load.sas_program(pattern))}</div>
      <div class="col"><h4>Generated PySpark</h4>{_pre(body)}</div>
    </div></div>"""


def verdict_html(title: str, report: parity.ParityReport) -> str:
    cls = "pass" if report.passed else "fail"
    word = "PASS" if report.passed else "FAIL"
    return _STYLE + f'<div class="s2p"><div class="verdict {cls}">{word} — {html.escape(title)}</div>{_pre(str(report))}</div>'


def check(pattern: str, output: pd.DataFrame, data_path) -> tuple[str, parity.ParityReport]:
    name, ref = answer_key(pattern, data_path)
    return name, parity.compare(ref, output, key=load.KEYS[pattern])


def scoreboard(spark, data_path) -> pd.DataFrame:
    """Every pattern: convert, run, check; and check its naive translation."""
    rows = []
    for pid in load.PATTERNS:
        conv = convert_pattern(pid)
        out = load.to_pandas(run_converted(pid, conv, spark, data_path))
        key_name, rep = check(pid, out, data_path)
        naive_result, naive_rows = "—", None
        if pid in naive.NAIVE:
            nrep = check(pid, naive.run(pid, data_path), data_path)[1]
            naive_result = "PASS (!)" if nrep.passed else "FAIL (caught)"
            naive_rows = nrep.mismatched_rows or abs(nrep.row_count[0] - nrep.row_count[1])
        rows.append({
            "pattern": pid,
            "program": load.PATTERNS[pid].sas_file,
            "steps": len(conv.reports),
            "translation": ", ".join(sorted({TRANSLATION.get(r.status, r.status) for r in conv.reports})),
            "rows": len(out),
            "answer key": key_name,
            "converted": "PASS" if rep.passed else "FAIL",
            "naive": naive_result,
            "naive rows wrong": naive_rows,
        })
    board = pd.DataFrame(rows)
    board["naive rows wrong"] = board["naive rows wrong"].astype("Int64")
    return board
