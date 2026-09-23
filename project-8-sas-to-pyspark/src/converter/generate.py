"""ir.Program -> a PySpark module.

The generator walks the program's steps in order, tracking for every dataset
its PySpark variable, its columns (with types) and the order its rows are in
- SAS semantics lean on physical row order, Spark has none, so the sort key
has to be carried forward by hand and turned into explicit window ordering.

Each DATA step is translated one of three ways, chosen by what its RETAINed
state does:

  vectorised   no state, or state a window function can express exactly
               (running totals, FIRST./LAST., LAG/DIF) -> withColumn / Window
  sequential   state that depends on its own previous computed value
               -> groupBy(BY).applyInPandas, walking rows in order
  unsupported  anything else -> the step raises NotImplementedError and the
               report says why. The converter never guesses.
"""
from __future__ import annotations

import textwrap
from dataclasses import dataclass, field

from . import ir
from .exprs import (ColumnEmitter, PyEmitter, Unsupported, contains, fmt_num, is_lag,
                    kind_of, py_literal, variables)

TYPE_OF_KIND = {"num": "double", "char": "string", "date": "date", "bool": "boolean"}


@dataclass
class DsInfo:
    var: str
    schema: dict[str, str]          # column -> spark type, in order
    sorted_by: list[str]


@dataclass
class StepReport:
    title: str
    lines: tuple[int, int]
    status: str = "converted"       # converted | sequential | unsupported | skipped
    notes: list[str] = field(default_factory=list)


@dataclass
class Conversion:
    code: str
    reports: list[StepReport]
    outputs: list[str]

    @property
    def ok(self) -> bool:
        return all(r.status != "unsupported" for r in self.reports)


def _walk(stmts):
    for s in stmts:
        yield s
        if isinstance(s, ir.If):
            yield from _walk(s.then)
            yield from _walk(s.else_ or [])
        elif isinstance(s, (ir.Block, ir.DoLoop)):
            yield from _walk(s.body)


def _exprs(s: ir.Stmt) -> list[ir.Expr]:
    if isinstance(s, (ir.Assign, ir.SumStmt)):
        return [s.expr]
    if isinstance(s, (ir.If, ir.SubsetIf)):
        return [s.cond]
    if isinstance(s, ir.DoLoop):
        return [s.start, s.stop] + ([s.by] if s.by else [])
    return []


def _reads(s: ir.Stmt, var: str) -> bool:
    return any(var in variables(e) for e in _exprs(s))


def _assigns(s: ir.Stmt) -> str | None:
    return s.target if isinstance(s, (ir.Assign, ir.SumStmt)) else None


def _is_first(e: ir.Expr, by: list[str]) -> int | None:
    """If e is FIRST.<by var>, the index of that BY variable."""
    if isinstance(e, ir.FirstLast) and e.which == "first" and e.var in by:
        return by.index(e.var)
    return None


# --------------------------------------------------------------------------
# Plans for stateful DATA steps
# --------------------------------------------------------------------------


@dataclass
class Accum:
    var: str
    kind: str                   # "sum" (sum statement) | "plus" (v = v + e)
    expr: ir.Expr
    start: ir.Expr | None       # value at the start of each group (None = missing)
    level: int | None           # BY index it resets on; None = never (whole dataset)
    update: ir.Stmt


def _reset_block(s: ir.Stmt, by: list[str], candidates: set[str]):
    """`if first.g then <v = const>;` or `... then do; v1 = c; v2 = .; end;`
    Returns (level, {var: value}) or None."""
    if not isinstance(s, ir.If) or s.else_:
        return None
    level = _is_first(s.cond, by)
    if level is None:
        return None
    body = [x for x in _walk(s.then) if not isinstance(x, ir.Block)]
    if not body or not all(isinstance(x, ir.Assign) and x.target in candidates
                           and isinstance(x.expr, (ir.Num, ir.Missing)) for x in body):
        return None
    return level, {x.target: x.expr for x in body}


def plan_accumulators(body: list[ir.Stmt], by: list[str], retained: dict) -> dict[str, Accum]:
    found = {}
    for v, init in retained.items():
        resets = [(i, r) for i, s in enumerate(body) if (r := _reset_block(s, by, set(retained))) and v in r[1]]
        updates = [(i, s) for i, s in enumerate(body) if _assigns(s) == v]
        nested = [s for s in _walk(body) if _assigns(s) == v and not any(s is u for _, u in updates)]
        nested = [s for s in nested if not any(s in _walk([body[i]]) for i, _ in resets)]
        if len(updates) != 1 or nested or len(resets) > 1:
            continue
        ui, u = updates[0]
        if isinstance(u, ir.SumStmt):
            kind, e = "sum", u.expr
        elif isinstance(u, ir.Assign) and isinstance(u.expr, ir.Binary) and u.expr.op == "+" \
                and ir.Var(v) in (u.expr.left, u.expr.right):
            kind = "plus"
            e = u.expr.right if u.expr.left == ir.Var(v) else u.expr.left
        else:
            continue
        if variables(e) & set(retained) or contains(e, is_lag):
            continue
        # v may only be read by its own update or after it
        if any(_reads(s, v) for i, s in enumerate(body[:ui]) if not any(i == ri for ri, _ in resets)):
            continue
        if resets and resets[0][0] > ui:
            continue
        if resets:
            level, values = resets[0][1]
            start = values[v]
        else:
            level, start = None, init if init is not None else (ir.Num(0) if kind == "sum" else None)
        if isinstance(start, ir.Missing):
            start = ir.Num(0) if kind == "sum" else None
        found[v] = Accum(v, kind, e, start, level, u)
    return found


# --------------------------------------------------------------------------
# Generator
# --------------------------------------------------------------------------


class Generator:
    def __init__(self, source_name: str, input_schemas: dict[str, dict[str, str]]):
        self.source_name = source_name
        self.input_schemas = input_schemas
        self.datasets: dict[str, DsInfo] = {}
        self.inputs: list[str] = []
        self.lines: list[str] = []
        self.reports: list[StepReport] = []
        self.created: list[str] = []
        self.n_temp = 0

    # -- output helpers -----------------------------------------------------

    def emit(self, text: str = "", indent: int = 1):
        for ln in text.splitlines() or [""]:
            self.lines.append(("    " * indent + ln) if ln else "")

    def dataset(self, ref: ir.DsRef) -> DsInfo:
        if ref.name in self.datasets:
            return self.datasets[ref.name]
        if ref.name in self.input_schemas:
            info = DsInfo(ref.name, dict(self.input_schemas[ref.name]), [])
            self.datasets[ref.name] = info
            self.inputs.append(ref.name)
            return info
        raise Unsupported(f"dataset {ref} is neither created earlier nor a known input")

    def define(self, name: str, info: DsInfo):
        self.datasets[name] = info
        if name not in self.created:
            self.created.append(name)

    def temp(self, prefix: str) -> str:
        self.n_temp += 1
        return f"_{prefix}{self.n_temp}"

    # -- program ------------------------------------------------------------

    def program(self, prog: ir.Program) -> Conversion:
        macro = [g for g in prog.globals if g.lstrip().startswith("%")]
        other = [g.split()[0].upper() for g in prog.globals if not g.lstrip().startswith("%")]
        if macro:
            self.reports.append(StepReport("SAS macro language", (0, 0), "unsupported", [
                f"macro statements are not supported ({macro[0].splitlines()[0][:60]} ...): "
                "resolve macros first (e.g. from the SAS log with MPRINT) and convert the expanded code"]))
        if other:
            self.reports.append(StepReport("global statements", (0, 0), "converted", [
                f"{', '.join(sorted(set(other)))} ignored (session settings, no effect on data values)"]))
        for step in prog.steps:
            title = _title(step)
            report = StepReport(title, (step.line_start, step.line_end))
            self.reports.append(report)
            mark = len(self.lines)
            self.emit()
            self.emit(f"# {'─' * 4} {title}  (sas lines {step.line_start}-{step.line_end}) {'─' * 4}")
            for ln in _source_comment(step):
                self.emit(f"# | {ln}" if ln.strip() else "# |")
            header_end = len(self.lines)
            try:
                self.step(step, report)
            except Unsupported as exc:
                del self.lines[header_end:]
                report.status = "unsupported"
                report.notes.append(str(exc))
                self.emit(f"# UNSUPPORTED: {exc}")
                self.emit(f'raise NotImplementedError("{title}: {str(exc)}")'.replace('\\', '\\\\'))
                for ref in _outputs(step):
                    self.define(ref.name, DsInfo(ref.name, {}, []))
            if report.status == "skipped":
                del self.lines[mark:]
        return Conversion(self.module(prog), self.reports, list(self.created))

    def module(self, prog: ir.Program) -> str:
        report_lines = []
        for r in self.reports:
            report_lines.append(f"[{r.status.upper():>11}] {r.title} (lines {r.lines[0]}-{r.lines[1]})")
            report_lines += [f"              - {n}" for n in r.notes]
        head = textwrap.dedent(f'''\
            """PySpark generated from {self.source_name} by the sas2databricks converter.

            Do not edit by hand. Regenerate with:
                python -m src.converter {self.source_name}

            Conversion report
            -----------------
            ''') + "\n".join(report_lines) + textwrap.dedent('''
            """
            # ruff: noqa
            import math
            from datetime import date

            import pandas as pd
            from pyspark.sql import DataFrame, SparkSession, Window
            from pyspark.sql import functions as F

            from src import sasrt as sas

            ''')
        head += f"INPUTS = {self.inputs!r}\nOUTPUTS = {self.created!r}\n\n\n"
        head += "def run(spark: SparkSession, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:\n"
        body = [f'    {name} = tables["{name}"]' for name in self.inputs]
        body += self.lines
        body.append("")
        body.append("    return {" + ", ".join(f'"{n}": {n}' for n in self.created) + "}")
        return head + "\n".join(body) + "\n"

    def step(self, step: ir.Step, report: StepReport):
        if isinstance(step, ir.ProcSort):
            self.proc_sort(step, report)
        elif isinstance(step, ir.ProcMeans):
            self.proc_means(step, report)
        elif isinstance(step, ir.DataStep):
            DataStepGen(self, step, report).run()
        else:
            raise Unsupported(f"{step.kind.upper()} is not supported by the converter")

    # -- dataset options ------------------------------------------------------

    def read(self, ref: ir.DsRef, report: StepReport) -> tuple[str, dict[str, str]]:
        """Code for a dataset reference with KEEP/DROP/RENAME/WHERE applied
        (in SAS's order for input datasets)."""
        info = self.dataset(ref)
        code, schema, o = info.var, dict(info.schema), ref.options
        if o.keep is not None:
            code += ".select(" + ", ".join(f'"{c}"' for c in o.keep) + ")"
            schema = {c: schema[c] for c in o.keep}
        if o.drop:
            code += ".drop(" + ", ".join(f'"{c}"' for c in o.drop) + ")"
            schema = {c: t for c, t in schema.items() if c not in o.drop}
        for old, new in o.rename.items():
            code += f'.withColumnRenamed("{old}", "{new}")'
            schema = {(new if c == old else c): t for c, t in schema.items()}
        if o.where is not None:
            em = ColumnEmitter(_SimpleCtx(schema))
            code += f".where({em.cond(o.where)})"
            report.notes += em_notes(em)
        return code, schema

    # -- PROC SORT ----------------------------------------------------------

    def proc_sort(self, step: ir.ProcSort, report: StepReport):
        if step.options:
            raise Unsupported(f"PROC SORT options {step.options} (e.g. NODUPKEY) are not supported")
        code, schema = self.read(step.data, report)
        target = step.out or step.data
        report.notes.append(
            f"PROC SORT dropped: Spark DataFrames have no row order. Sort key "
            f"({', '.join(step.by)}) kept as metadata and used to order windows downstream.")
        if code != target.name:
            self.emit(f"{target.name} = {code}  # sort key carried forward: {step.by}")
        else:
            self.emit(f"# {target.name}: sort key carried forward: {step.by}")
        self.define(target.name, DsInfo(target.name, schema, list(step.by)))

    # -- PROC MEANS / SUMMARY -----------------------------------------------

    def proc_means(self, step: ir.ProcMeans, report: StepReport):
        if step.unsupported:
            raise Unsupported(f"PROC {step.proc.upper()}: {', '.join(step.unsupported)}")
        if step.class_ and "nway" not in step.options:
            raise Unsupported(
                f"PROC {step.proc.upper()} without NWAY writes one row per CLASS combination "
                "(_TYPE_ levels); translate with rollup/cube by hand")
        code, schema = self.read(step.data, report)
        agg_fn = {"n": "F.count({v})", "nmiss": "F.sum(F.col({v}).isNull().cast('int'))",
                  "sum": "F.sum({v})", "mean": "F.avg({v})", "min": "F.min({v})",
                  "max": "F.max({v})", "std": "F.stddev_samp({v})", "var": "F.var_samp({v})"}
        for out in step.outputs:
            o = out.out.options
            aggs, out_schema = [], {c: schema[c] for c in step.class_}
            freq_name = o.rename.get("_freq_", "_freq_")
            keep_freq = not (o.drop and "_freq_" in o.drop) and (o.keep is None or "_freq_" in o.keep)
            if keep_freq:
                aggs.append(f'F.count(F.lit(1)).alias("{freq_name}"),  # _FREQ_: all rows in the group')
                out_schema[freq_name] = "bigint"
            for stat, vars_, names in out.stats:
                vars_ = vars_ or step.var
                if len(names) != len(vars_):
                    raise Unsupported(f"OUTPUT {stat}= needs one name per analysis variable")
                for v, name in zip(vars_, names):
                    note = "  # N: non-missing values only" if stat == "n" else ""
                    aggs.append(f'{agg_fn[stat].format(v=repr(v).replace(chr(39), chr(34)))}.alias("{name}"),{note}')
                    out_schema[name] = "bigint" if stat in ("n", "nmiss") else "double"
            k = len(step.class_)
            lines = [f"{out.out.name} = (", f"    {code}"]
            if step.class_ and "missing" not in step.options:
                cond = " & ".join(f'F.col("{c}").isNotNull()' for c in step.class_)
                lines.append(f"    .where({cond})  # CLASS rows with missing values are excluded")
                report.notes.append(
                    f"rows with a missing CLASS value ({', '.join(step.class_)}) dropped, as PROC "
                    f"{step.proc.upper()} does without MISSING; Spark's groupBy would keep a NULL group")
            if step.class_:
                lines.append(f"    .groupBy({', '.join(repr(c).replace(chr(39), chr(34)) for c in step.class_)})")
            lines.append("    .agg(")
            lines += [f"        {a}" for a in aggs]
            lines.append("    )")
            keep_type = not (o.drop and "_type_" in o.drop) and (o.keep is None or "_type_" in o.keep)
            if keep_type:
                lines.append(f'    .withColumn("{o.rename.get("_type_", "_type_")}", F.lit({2 ** k - 1}))')
                out_schema[o.rename.get("_type_", "_type_")] = "int"
            lines.append(")")
            self.emit("\n".join(lines))
            if any(s == "n" for s, _, _ in out.stats) and keep_freq:
                report.notes.append("N(var) -> F.count(var) (non-missing); _FREQ_ -> F.count(*) (rows)")
            self.define(out.out.name, DsInfo(out.out.name, out_schema, list(step.class_)))


class _SimpleCtx:
    def __init__(self, schema):
        self.schema = schema

    def first_last(self, which, var):
        raise Unsupported(f"{which.upper()}.{var} outside a DATA step with BY")

    def lag(self, arg):
        raise Unsupported("LAG/DIF in a dataset option")


def em_notes(em: ColumnEmitter) -> list[str]:
    return list(dict.fromkeys(em.notes))


def _title(step: ir.Step) -> str:
    if isinstance(step, ir.DataStep):
        return "DATA " + " ".join(str(o) for o in step.outputs) if step.outputs else "DATA _NULL_"
    if isinstance(step, ir.ProcSort):
        return f"PROC SORT {step.data}" + (f" -> {step.out}" if step.out else "")
    if isinstance(step, ir.ProcMeans):
        return f"PROC {step.proc.upper()} {step.data} -> " + ", ".join(str(o.out) for o in step.outputs)
    return step.kind.upper()


def _outputs(step: ir.Step) -> list[ir.DsRef]:
    if isinstance(step, ir.DataStep):
        return step.outputs
    if isinstance(step, ir.ProcSort):
        return [step.out or step.data]
    if isinstance(step, ir.ProcMeans):
        return [o.out for o in step.outputs]
    return []


def _source_comment(step: ir.Step) -> list[str]:
    """The step's SAS source, with DATALINES content collapsed."""
    out, in_data = [], False
    for ln in step.source.splitlines():
        low = ln.strip().lower()
        if in_data:
            if low == ";":
                in_data = False
                out.append(ln)
            continue
        out.append(ln)
        if low.startswith(("datalines", "cards")):
            in_data = True
            out.append("    ... (in-stream data, see createDataFrame below)")
    return out


# --------------------------------------------------------------------------
# DATA step
# --------------------------------------------------------------------------

_SKIP = (ir.Set, ir.Merge, ir.By, ir.Retain, ir.Length, ir.Keep, ir.Drop)


class DataStepGen:
    def __init__(self, g: Generator, step: ir.DataStep, report: StepReport):
        self.g, self.step, self.report = g, step, report
        self.notes = report.notes
        top = step.stmts
        self.by = next((s.vars for s in top if isinstance(s, ir.By)), [])
        self.keep = next((s.names for s in top if isinstance(s, ir.Keep)), None)
        self.drop = [n for s in top if isinstance(s, ir.Drop) for n in s.names]
        self.lengths = {n: (c, l) for s in top if isinstance(s, ir.Length) for n, c, l in s.items}
        self.retained: dict[str, ir.Expr | None] = {}
        for s in top:
            if isinstance(s, ir.Retain):
                self.retained.update(dict(s.items))
        for s in _walk(top):
            if isinstance(s, ir.SumStmt):
                self.retained.setdefault(s.target, ir.Num(0))
        self.body = [s for s in top if not isinstance(s, _SKIP)]
        for s in _walk(self.body):
            if isinstance(s, _SKIP):
                raise Unsupported(f"{type(s).__name__.upper()} statement inside a conditional block")
        self.temps: set[str] = set()
        self.flags: set[str] = set()

    # -- entry ----------------------------------------------------------------

    def run(self):
        st = self.step
        if len(st.outputs) != 1:
            raise Unsupported("DATA steps writing several datasets are not supported")
        self.out = st.outputs[0]
        if any(isinstance(s, ir.Datalines) for s in st.stmts):
            return self.datalines()
        for s in _walk(self.body):
            if isinstance(s, ir.Other) and not s.ignorable:
                raise Unsupported(f"{s.keyword.upper()} statement: {s.text}")
            if isinstance(s, ir.Other):
                self.notes.append(f"{s.keyword.upper()} ignored (no effect on data values)")

        src = [s for s in st.stmts if isinstance(s, (ir.Set, ir.Merge))]
        if len(src) != 1:
            raise Unsupported("DATA step needs exactly one SET or MERGE")
        if isinstance(src[0], ir.Merge):
            self.merge(src[0])
        else:
            self.set(src[0])

        self.accums = plan_accumulators(self.body, self.by, self.retained)
        unresolved = [v for v in self.retained if v not in self.accums]
        if unresolved:
            return self.sequential(unresolved)
        self.vectorised()

    # -- sources ----------------------------------------------------------------

    def set(self, s: ir.Set):
        parts = [self.g.read(ref, self.report) for ref in s.datasets]
        if len(parts) == 1:
            code, self.schema = parts[0]
            self.sorted_by = list(self.g.dataset(s.datasets[0]).sorted_by)
        else:
            code = parts[0][0] + "".join(f".unionByName({c}, allowMissingColumns=True)" for c, _ in parts[1:])
            self.schema = {}
            for _, sch in parts:
                self.schema.update({c: t for c, t in sch.items() if c not in self.schema})
            self.sorted_by = []
            self.notes.append("SET with several datasets -> unionByName (stacked; row order not preserved)")
        if any(r.options.in_ for r in s.datasets):
            raise Unsupported("IN= on SET is not supported")
        self.lines = [f"df = {code}"]
        self.source_var = code

    def merge(self, m: ir.Merge):
        if len(m.datasets) != 2:
            raise Unsupported("MERGE of more than two datasets")
        if not self.by:
            raise Unsupported("MERGE without BY is a positional one-to-one merge, not a join")
        (lcode, lsch), (rcode, rsch) = (self.g.read(r, self.report) for r in m.datasets)
        lin = m.datasets[0].options.in_ or "_in_left"
        rin = m.datasets[1].options.in_ or "_in_right"
        shared = [c for c in rsch if c in lsch and c not in self.by]

        # `if in_a;` / `if in_b;` / `if in_a and in_b;` choose the join type
        how, consumed = "full", None
        for s in self.body:
            if isinstance(s, ir.SubsetIf):
                v = variables(s.cond)
                if s.cond == ir.Var(lin):
                    how = "left"
                elif s.cond == ir.Var(rin):
                    how = "right"
                elif isinstance(s.cond, ir.Binary) and s.cond.op == "and" and v == {lin, rin} \
                        and {type(s.cond.left), type(s.cond.right)} == {ir.Var}:
                    how = "inner"
                else:
                    continue
                consumed = s
                break
        if consumed is not None:
            self.body.remove(consumed)

        by_list = ", ".join(f'"{b}"' for b in self.by)
        rsel = rcode + "".join(f'.withColumnRenamed("{c}", "_r_{c}")' for c in shared)
        lines = [
            f'sas.assert_no_many_to_many({lcode}, {rcode}, [{by_list}], "{_title(self.step)}")',
            "df = (",
            f'    {lcode}.withColumn("{lin}", F.lit(True))',
            f'    .join({rsel}.withColumn("{rin}", F.lit(True)), on=[{by_list}], how="{how}")',
            f'    .withColumn("{lin}", F.coalesce(F.col("{lin}"), F.lit(False)))',
            f'    .withColumn("{rin}", F.coalesce(F.col("{rin}"), F.lit(False)))',
            ")",
        ]
        for c in shared:
            lines.append(f'df = df.withColumn("{c}", F.when(F.col("{rin}"), F.col("_r_{c}")).otherwise(F.col("{c}")))'
                         f'.drop("_r_{c}")  # same column in both: the dataset read last wins')
        self.lines = lines
        self.schema = {**{b: lsch.get(b, rsch.get(b)) for b in self.by},
                       **{c: t for c, t in lsch.items() if c not in self.by},
                       **{c: t for c, t in rsch.items() if c not in self.by and c not in lsch},
                       lin: "boolean", rin: "boolean"}
        self.flags |= {lin, rin}
        self.sorted_by = list(self.by)
        via = {"left": f"`if {lin};` -> left join", "right": f"`if {rin};` -> right join",
               "inner": f"`if {lin} and {rin};` -> inner join", "full": "no IN= subsetting -> full outer join"}[how]
        self.notes.append(f"MERGE BY {', '.join(self.by)}: {via}; runtime guard stops if both sides "
                          "repeat a BY value (SAS pairs rows, a join would multiply them)")
        if shared:
            self.notes.append(f"columns in both inputs ({', '.join(shared)}): value from the dataset listed last")

    # -- windows ----------------------------------------------------------------

    def order_within(self, level: int | None) -> list[str]:
        """Sort columns that order rows inside a BY group at `level`."""
        if self.by and self.sorted_by[:len(self.by)] != self.by:
            self.warn(f"input is not known to be sorted by {self.by} (SAS would stop with "
                      "'BY variables are not properly sorted'); ordering by BY variables only")
            return []
        part = self.by[:level + 1] if level is not None else []
        return [c for c in self.sorted_by if c not in part]

    def window(self, level: int | None, running: bool = False) -> str:
        part = self.by[:level + 1] if level is not None else []
        order = self.order_within(level) or part
        name = "w_" + ("_".join(part) if part else "all") + ("_run" if running else "")
        if name not in self.windows:
            code = "Window"
            if part:
                code += ".partitionBy(" + ", ".join(f'"{c}"' for c in part) + ")"
            else:
                self.warn("window over the whole dataset (no BY partition): runs in a single task")
            code += ".orderBy(" + ", ".join(f'"{c}"' for c in order) + ")"
            if running:
                code += ".rowsBetween(Window.unboundedPreceding, Window.currentRow)"
            if not self.order_within(level):
                self.warn(f"row order inside {part or 'the dataset'} is not determined by a sort key; "
                          "SAS uses physical order, Spark has none")
            self.windows[name] = code
            comment = ""
            if running:
                comment = "  # ROWS frame: one row at a time, like SAS. RANGE would give tied rows one total"
            self.lines.append(f"{name} = {code}{comment}")
        return name

    def warn(self, msg: str):
        if f"WARNING: {msg}" not in self.notes:
            self.notes.append(f"WARNING: {msg}")

    # -- ColumnEmitter context -------------------------------------------------

    def first_last(self, which: str, var: str) -> str:
        if var not in self.by:
            raise Unsupported(f"{which.upper()}.{var} but {var} is not a BY variable")
        k = self.by.index(var)
        rn, n = f"_rn_{var}", f"_n_{var}"
        if rn not in self.temps:
            w = self.window(k)
            self.lines.append(f'df = df.withColumn("{rn}", F.row_number().over({w}))')
            part = ", ".join(f'"{c}"' for c in self.by[:k + 1])
            self.lines.append(f'df = df.withColumn("{n}", F.count(F.lit(1)).over(Window.partitionBy({part})))')
            self.temps |= {rn, n}
            self.schema[rn] = self.schema[n] = "int"
        return f'(F.col("{rn}") == 1)' if which == "first" else f'(F.col("{rn}") == F.col("{n}"))'

    def lag(self, arg) -> str:
        return f"F.lag({arg.src if not arg.literal else f'F.lit({arg.src})'}).over({self.lag_window})"

    # -- vectorised -----------------------------------------------------------

    def vectorised(self):
        self.windows: dict[str, str] = {}
        self.em = ColumnEmitter(self)
        self.consumed: set[int] = set()
        self.output_done = False

        # accumulators: absorb their reset blocks
        for a in self.accums.values():
            for s in self.body:
                r = _reset_block(s, self.by, set(self.retained) | self.lag_targets())
                if r and a.var in r[1]:
                    self.consumed.add(id(s))
        self.plan_lag()
        self.char_lengths()

        # FIRST./LAST. describe the rows as read, before any subsetting IF
        for s in self.body:
            if id(s) not in self.consumed:
                for x in _walk([s]):
                    for e in _exprs(x):
                        for fl in _iter_firstlast(e):
                            self.first_last(fl.which, fl.var)

        self.stmts(self.body, [])
        for n in dict.fromkeys(self.em.notes):
            self.notes.append(n)
        self.finish()

    def lag_targets(self) -> set[str]:
        return {s.target for s in self.body if isinstance(s, ir.Assign) and contains(s.expr, is_lag)}

    def plan_lag(self):
        self.lag_window = None
        lag_stmts = [s for s in _walk(self.body) if any(contains(e, is_lag) for e in _exprs(s))]
        if not lag_stmts:
            return
        top = [s for s in self.body if isinstance(s, ir.Assign) and contains(s.expr, is_lag)]
        if len(top) != len(lag_stmts):
            raise Unsupported("LAG/DIF executed conditionally: SAS LAG is a queue that only "
                              "advances when called, so it is not 'the previous row'")
        targets = {s.target for s in top}
        blanks = [(i, r) for i, s in enumerate(self.body)
                  if (r := _reset_block(s, self.by, targets | set(self.retained)))
                  and all(isinstance(v, ir.Missing) for t, v in r[1].items() if t in targets)
                  and targets <= set(r[1])]
        if blanks:
            i, (level, _) = blanks[0]
            last_lag = max(self.body.index(s) for s in top)
            if i > last_lag:
                self.consumed.add(id(self.body[i]))
                self.lag_window = self.window(level)
                self.notes.append(
                    f"LAG/DIF blanked on FIRST.{self.by[level]} -> F.lag over a window partitioned by "
                    f"{', '.join(self.by[:level + 1])} (SAS calls LAG on every row, so it is the previous row)")
                return
        self.lag_window = self.window(None)
        self.notes.append("LAG/DIF not reset per BY group -> F.lag over the whole dataset in sort order")

    def char_lengths(self):
        """SAS fixes a character variable's length at compile time: LENGTH, else
        the first assignment it compiles. Longer values are truncated."""
        self.char_len = {n: l for n, (c, l) in self.lengths.items() if c}
        for s in _walk(self.body):
            if isinstance(s, ir.Assign) and isinstance(s.expr, ir.Str) \
                    and s.target not in self.char_len and s.target not in self.schema:
                self.char_len[s.target] = len(s.expr.value)

    def value(self, target: str, e: ir.Expr) -> str:
        """Code for a value stored into `target` (applying SAS character truncation)."""
        if isinstance(e, ir.Str) and target in self.char_len and len(e.value) > self.char_len[target]:
            cut = e.value[:self.char_len[target]]
            self.notes.append(
                f"'{e.value}' is truncated to '{cut}': {target} is ${self.char_len[target]} "
                "(from its first assignment - add a LENGTH statement if that is not intended)")
            e = ir.Str(cut)
        return self.em.value(e).src

    def set_type(self, target: str, e: ir.Expr):
        c = self.em.value(e)
        self.schema[target] = TYPE_OF_KIND[c.kind] if c.kind != "num" or target not in self.schema \
            else self.schema[target]

    def assign(self, target: str, code: str, stack: list[str], e: ir.Expr):
        if stack:
            cond = " & ".join(stack)
            code = f"F.when({cond}, {code})"
            if target in self.schema:
                code += f'.otherwise(F.col("{target}"))'
        elif code.startswith(("'", '"')) or code.lstrip("-").replace(".", "", 1).isdigit() or code.startswith("date("):
            code = f"F.lit({code})"
        self.lines.append(f'df = df.withColumn("{target}", {code})')
        self.set_type(target, e)

    def chain(self, s: ir.If):
        """if/else-if/else that assigns one variable in every branch -> one F.when chain."""
        branches, node, target = [], s, None
        while True:
            if len(node.then) != 1 or not isinstance(node.then[0], ir.Assign):
                return None
            t = node.then[0].target
            if target not in (None, t):
                return None
            target = t
            branches.append((node.cond, node.then[0].expr))
            if node.else_ is None:
                return target, branches, None
            if len(node.else_) == 1 and isinstance(node.else_[0], ir.If):
                node = node.else_[0]
                continue
            if len(node.else_) == 1 and isinstance(node.else_[0], ir.Assign) and node.else_[0].target == target:
                return target, branches, node.else_[0].expr
            return None

    def stmts(self, stmts: list[ir.Stmt], stack: list[str]):
        for s in stmts:
            if id(s) in self.consumed:
                continue
            if self.output_done:
                raise Unsupported(f"statement after the OUTPUT loop (line {s.line})")
            self.stmt(s, stack)

    def stmt(self, s: ir.Stmt, stack: list[str]):
        acc = next((a for a in self.accums.values() if a.update is s), None)
        if acc:
            return self.accumulate(acc, stack)
        if isinstance(s, ir.Assign):
            if contains(s.expr, is_lag) and stack:
                raise Unsupported("LAG/DIF inside a condition")
            return self.assign(s.target, self.value(s.target, s.expr), stack, s.expr)
        if isinstance(s, ir.If):
            ch = self.chain(s)
            if ch:
                target, branches, default = ch
                parts = [f"F.when({self.em.cond(branches[0][0])}, {self.value(target, branches[0][1])})"]
                parts += [f".when({self.em.cond(c)}, {self.value(target, v)})" for c, v in branches[1:]]
                if default is not None:
                    parts.append(f".otherwise({self.value(target, default)})")
                elif target in self.schema:
                    parts.append(f'.otherwise(F.col("{target}"))')
                code = parts[0] if len(parts) == 1 else "(\n    " + "\n    ".join(parts) + "\n)"
                self.assign(target, code, stack, branches[0][1])
                return
            cond = self.em.cond(s.cond)
            assigned = {_assigns(x) for x in _walk(s.then + (s.else_ or []))} - {None}
            if variables(s.cond) & assigned:
                name = self.g.temp("if")
                self.lines.append(f'df = df.withColumn("{name}", {cond})')
                self.temps.add(name)
                self.schema[name] = "boolean"
                cond = f'F.col("{name}")'
            self.stmts(s.then, stack + [cond])
            if s.else_:
                self.stmts(s.else_, stack + [f"~{cond}"])
            return
        if isinstance(s, ir.SubsetIf):
            c = self.em.cond(s.cond)
            self.lines.append(f"df = df.where({c})" if not stack else
                              f"df = df.where(~({' & '.join(stack)}) | {c})")
            return
        if isinstance(s, ir.Delete):
            self.lines.append("df = df.where(F.lit(False))" if not stack else
                              f"df = df.where(~({' & '.join(stack)}))")
            return
        if isinstance(s, ir.Block):
            return self.stmts(s.body, stack)
        if isinstance(s, ir.DoLoop):
            return self.do_output(s, stack)
        if isinstance(s, ir.Output):
            raise Unsupported(f"OUTPUT at line {s.line} outside a DO loop")
        if isinstance(s, ir.Other):
            return
        raise Unsupported(f"{type(s).__name__} at line {s.line}")

    def do_output(self, s: ir.DoLoop, stack: list[str]):
        outs = [x for x in _walk(s.body) if isinstance(x, ir.Output)]
        if stack or len(outs) != 1 or s.body[-1] is not outs[0] or outs[0].dataset \
                or not all(isinstance(x, ir.Assign) for x in s.body[:-1]):
            raise Unsupported("only `DO i = a TO b; <assignments>; OUTPUT; END;` is supported")
        if s.by is not None and not isinstance(s.by, ir.Num):
            raise Unsupported("DO ... BY with a non-constant step")
        args = [self.em.column(s.start), self.em.column(s.stop)]
        if s.by is not None:
            args.append(fmt_num(s.by.value))
        self.lines.append(f'df = df.withColumn("{s.var}", F.explode(sas.do_to({", ".join(args)})))')
        self.schema[s.var] = "int"
        self.notes.append(f"DO {s.var} = ... TO ...; OUTPUT; -> explode(sequence): one row per iteration "
                          "(an empty range gives no rows, as in SAS)")
        self.stmts(s.body[:-1], [])
        self.sorted_by = self.sorted_by + [s.var]
        self.output_done = True

    def accumulate(self, a: Accum, stack: list[str]):
        if stack:
            raise Unsupported(f"running total {a.var} updated inside a condition")
        e = self.em.column(a.expr)
        start = None if a.start is None else self.em.emit(a.start).src
        reset = f"reset on FIRST.{self.by[a.level]}" if a.level is not None else "never reset"
        if a.kind == "sum" and a.expr == ir.Num(1) and start in ("0", None):
            w = self.window(a.level)
            code = f"F.row_number().over({w})"
            self.notes.append(f"`{a.var} + 1` (sum statement, implied RETAIN, {reset}) -> row_number()")
        elif a.kind == "sum":
            w = self.window(a.level, running=True)
            code = f"F.coalesce(F.sum({e}).over({w}), F.lit(0))"
            if start not in ("0", None):
                code = f"{start} + {code}"
            self.notes.append(f"`{a.var} + ...` (sum statement, {reset}) -> running SUM, missing counted as 0")
        else:
            w = self.window(a.level, running=True)
            total = f"F.sum({e}).over({w})" if start in ("0",) else \
                (f"{start} + F.sum({e}).over({w})" if start else 'F.lit(None).cast("double")')
            code = (f"F.when(F.max({e}.isNull().cast('int')).over({w}) == 1, F.lit(None))"
                    f"\n    .otherwise({total})")
            code = "(\n    " + code + "\n)"
            self.notes.append(
                f"RETAIN {a.var}; {a.var} = {a.var} + ... ({reset}) -> running SUM over a ROWS window "
                "ordered by the full sort key; `+` keeps a missing total missing for the rest of the group")
        self.lines.append(f'df = df.withColumn("{a.var}", {code})')
        self.schema[a.var] = "double"

    def finish(self):
        cols = self.keep if self.keep is not None else [
            c for c in self.schema if c not in self.drop and c not in self.temps and c not in self.flags]
        missing = [c for c in cols if c not in self.schema]
        if missing:
            raise Unsupported(f"KEEP names variables that are never defined: {missing}")
        o = self.out.options
        if o.keep is not None:
            cols = [c for c in cols if c in o.keep]
        if o.drop:
            cols = [c for c in cols if c not in o.drop]
        sel = ", ".join(f'"{c}"' if c not in o.rename else f'F.col("{c}").alias("{o.rename[c]}")' for c in cols)
        self.lines.append(f"{self.out.name} = df.select({sel})")
        self.g.emit("\n".join(self.lines))
        schema = {o.rename.get(c, c): self.schema[c] for c in cols}
        self.g.define(self.out.name, DsInfo(self.out.name, schema,
                                            [o.rename.get(c, c) for c in self.sorted_by if c in cols]))

    # -- sequential fallback ------------------------------------------------------

    def sequential(self, unresolved: list[str]):
        if any(contains(e, is_lag) for s in _walk(self.body) for e in _exprs(s)):
            raise Unsupported("LAG/DIF together with self-referential RETAIN")
        if self.flags:
            raise Unsupported("self-referential RETAIN in a MERGE step")
        if not self.by:
            raise Unsupported(
                f"RETAIN {', '.join(unresolved)} depends on its own previous value across the whole "
                "dataset (no BY group to parallelise over)")
        levels = []
        for v in self.retained:
            first_use = next((i for i, s in enumerate(self.body) if _reads(s, v) or _assigns(s) == v
                              or any(_reads(x, v) or _assigns(x) == v for x in _walk([s]))), None)
            s = self.body[first_use] if first_use is not None else None
            lvl = _is_first(s.cond, self.by) if isinstance(s, ir.If) and not s.else_ else None
            if lvl is None or not any(_assigns(x) == v for x in s.then):
                raise Unsupported(f"RETAIN {v} is not reset at the start of a BY group before it is used; "
                                  "groups are not independent")
            levels.append(lvl)
        part = self.by[:min(levels) + 1]
        order = self.order_within(min(levels)) or []
        sort_cols = part + order

        py = PyEmitter()
        name = f"_{self.out.name}_rows"
        computed = []
        for s in _walk(self.body):
            t = _assigns(s) or (s.var if isinstance(s, ir.DoLoop) else None)
            if t and t not in self.schema and t not in self.retained and t not in computed:
                computed.append(t)
        char = {n for n, (c, _) in self.lengths.items() if c} | {
            s.target for s in _walk(self.body) if isinstance(s, ir.Assign) and isinstance(s.expr, ir.Str)}
        miss = lambda v: "None" if v in char else "math.nan"   # noqa: E731
        init = {v: (py_literal(e) if isinstance(e, ir.LITERALS) else miss(v)) for v, e in self.retained.items()}

        fl = sorted({(e.which, e.var) for s in _walk(self.body) for x in _exprs(s)
                     for e in _iter_firstlast(x)})
        explicit_output = any(isinstance(s, ir.Output) for s in _walk(self.body))
        out_cols = self.keep if self.keep is not None else [
            c for c in list(self.schema) + computed + list(self.retained) if c not in self.drop]
        out_cols = list(dict.fromkeys(out_cols))

        fn = [f"def {name}(pdf: pd.DataFrame) -> pd.DataFrame:",
              f"    rows = pdf.sort_values({sort_cols!r}, kind=\"mergesort\").to_dict(\"records\")",
              "    out, pdv = [], {}",
              "    for i, row in enumerate(rows):",
              "        # next iteration: read a row; computed variables start missing, RETAINed ones carry over",
              "        pdv = {**row, " + ", ".join(
                  [f'"{c}": {miss(c)}' for c in computed] +
                  [f'"{v}": pdv.get("{v}", {init[v]})' for v in self.retained]) + "}"]
        for which, var in fl:
            k = self.by.index(var)
            keys = repr(self.by[:k + 1])
            other = "i - 1" if which == "first" else "i + 1"
            edge = "i == 0" if which == "first" else "i == len(rows) - 1"
            fn.append(f"        {which}_{var} = {edge} or any(rows[{other}][c] != row[c] for c in {keys})")
        body = []
        self.py_stmts(self.body, py, body, 2, out_cols)
        fn += body
        if not explicit_output:
            fn.append(f"        out.append({{c: pdv[c] for c in {out_cols!r}}})  # implicit OUTPUT")
        fn.append(f"    return pd.DataFrame(out, columns={out_cols!r})")

        types = {c: self.schema.get(c, "string" if c in char else ("int" if any(
            isinstance(s, ir.DoLoop) and s.var == c for s in _walk(self.body)) else "double")) for c in out_cols}
        ddl = ", ".join(f"{c} {t}" for c, t in types.items())
        part_list = ", ".join(f'"{c}"' for c in part)
        self.lines += fn
        self.lines.append(f'{self.out.name} = df.groupBy({part_list}).applyInPandas({name}, schema="{ddl}")')
        self.report.status = "sequential"
        self.notes.append(
            f"{', '.join(unresolved)} depends on its own previous computed value (self-referential RETAIN): "
            "no window function can express it. Each BY group is independent (reset on FIRST.), so groups "
            f"run in parallel via groupBy({', '.join(part)}).applyInPandas and rows are walked in "
            f"({', '.join(sort_cols)}) order")
        self.g.emit("\n".join(self.lines))
        self.g.define(self.out.name, DsInfo(self.out.name, types, [c for c in sort_cols if c in out_cols]))

    def py_stmts(self, stmts, py: PyEmitter, out: list[str], ind: int, out_cols):
        pad = "    " * ind
        if not stmts:
            out.append(pad + "pass")
        for s in stmts:
            if isinstance(s, ir.Assign):
                out.append(f'{pad}pdv["{s.target}"] = {py.emit(s.expr)}')
            elif isinstance(s, ir.SumStmt):
                out.append(f'{pad}pdv["{s.target}"] = sas.py_sum_stmt(pdv["{s.target}"], {py.emit(s.expr)})')
            elif isinstance(s, ir.If):
                out.append(f"{pad}if {py.truth(s.cond)}:")
                self.py_stmts(s.then, py, out, ind + 1, out_cols)
                if s.else_:
                    out.append(f"{pad}else:")
                    self.py_stmts(s.else_, py, out, ind + 1, out_cols)
            elif isinstance(s, ir.SubsetIf):
                out.append(f"{pad}if not {py.truth(s.cond)}:")
                out.append(f"{pad}    continue")
            elif isinstance(s, ir.Delete):
                out.append(f"{pad}continue")
            elif isinstance(s, ir.Block):
                self.py_stmts(s.body, py, out, ind, out_cols)
            elif isinstance(s, ir.DoLoop):
                step = f", {py.emit(s.by)}" if s.by else ""
                out.append(f'{pad}for pdv["{s.var}"] in sas.py_do_range({py.emit(s.start)}, {py.emit(s.stop)}{step}):')
                self.py_stmts(s.body, py, out, ind + 1, out_cols)
            elif isinstance(s, ir.Output):
                out.append(f"{pad}out.append({{c: pdv[c] for c in {out_cols!r}}})")
            elif isinstance(s, ir.Other):
                continue
            else:
                raise Unsupported(f"{type(s).__name__} in a sequential step")

    # -- DATALINES ----------------------------------------------------------------

    def datalines(self):
        allowed = (ir.Length, ir.Input, ir.Datalines, ir.Keep, ir.Drop)
        extra = [s for s in self.step.stmts if not isinstance(s, allowed)]
        if extra:
            raise Unsupported(f"statements other than LENGTH/INPUT alongside DATALINES (line {extra[0].line})")
        inp = next(s for s in self.step.stmts if isinstance(s, ir.Input))
        data = next(s for s in self.step.stmts if isinstance(s, ir.Datalines))
        rows, cut = [], set()
        for ln in data.lines:
            vals = ln.split()
            if len(vals) != len(inp.vars):
                raise Unsupported(f"in-stream line {ln!r} does not have {len(inp.vars)} values")
            row = []
            for (name, is_char), v in zip(inp.vars, vals):
                if is_char:
                    n = self.lengths.get(name, (True, 8))[1]
                    if len(v) > n:
                        cut.add(f"{v} -> {v[:n]}")
                        v = v[:n]
                    row.append(repr(v))
                else:
                    row.append("None" if v == "." else fmt_num(float(v)))
            rows.append("(" + ", ".join(row) + (",)" if len(row) == 1 else ")"))
        schema = {name: "string" if c else "double" for name, c in inp.vars}
        ddl = ", ".join(f"{c} {t}" for c, t in schema.items())
        lines = [f"{self.out.name} = spark.createDataFrame(", "    ["]
        lines += [f"        {r}," for r in rows]
        lines += ["    ],", f'    "{ddl}",', ")"]
        self.g.emit("\n".join(lines))
        self.notes.append(f"DATALINES ({len(rows)} rows) -> spark.createDataFrame")
        if cut:
            self.notes.append(f"WARNING: values longer than their LENGTH are truncated, as SAS does: {sorted(cut)}")
        self.g.define(self.out.name, DsInfo(self.out.name, schema, []))


def _iter_firstlast(e: ir.Expr):
    found = []
    contains(e, lambda x: isinstance(x, ir.FirstLast) and not found.append(x))
    return found


def generate(prog: ir.Program, source_name: str, input_schemas: dict[str, dict[str, str]]) -> Conversion:
    return Generator(source_name, input_schemas).program(prog)
