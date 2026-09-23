"""SAS expression -> Python source code, two dialects.

ColumnEmitter  -> a PySpark Column expression   (vectorised steps)
PyEmitter      -> a plain Python expression      (sequential fallback)

Both encode SAS semantics rather than the target's defaults. The headline
one: a missing value is SMALLER than every number and comparisons never
return NULL. Where the rule is short it is written inline so it is visible
in the generated code, e.g. SAS `dti < 20` becomes

    (F.col("dti").isNull() | (F.col("dti") < 20))
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from . import ir


class Unsupported(Exception):
    """A construct the converter will not translate - reported, never guessed."""


NUMERIC_TYPES = {"double", "float", "int", "bigint", "long", "smallint", "tinyint"}


def kind_of(spark_type: str) -> str:
    if spark_type == "string":
        return "char"
    if spark_type == "date":
        return "date"
    if spark_type == "boolean":
        return "bool"
    return "num"


def fmt_num(v: float) -> str:
    return str(int(v)) if float(v).is_integer() and abs(v) < 1e15 else repr(float(v))


def py_literal(e: ir.Expr) -> str:
    if isinstance(e, ir.Num):
        return fmt_num(e.value)
    if isinstance(e, ir.Str):
        return repr(e.value)
    if isinstance(e, ir.Date):
        return f"date({e.value.year}, {e.value.month}, {e.value.day})"
    raise TypeError(e)


def contains(e: ir.Expr, pred: Callable[[ir.Expr], bool]) -> bool:
    if pred(e):
        return True
    kids = {
        ir.Unary: lambda x: [x.operand],
        ir.Binary: lambda x: [x.left, x.right],
        ir.In: lambda x: [x.operand, *x.items],
        ir.Call: lambda x: x.args,
    }.get(type(e), lambda x: [])(e)
    return any(contains(k, pred) for k in kids)


def variables(e: ir.Expr) -> set[str]:
    found: set[str] = set()
    contains(e, lambda x: isinstance(x, ir.Var) and not found.add(x.name))
    return found


def is_lag(e: ir.Expr) -> bool:
    return isinstance(e, ir.Call) and e.name in ("lag", "lag1", "dif", "dif1")


@dataclass
class Code:
    src: str
    kind: str               # num | char | date | bool
    literal: bool = False   # plain Python literal (not yet a Column)


_CMP = {"<": "lt", "<=": "le", ">": "gt", ">=": "ge"}
_FLIP = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "=", "ne": "ne"}


class ColumnEmitter:
    """ctx must provide:
        schema: dict[str, str]                 column -> spark type
        first_last(which, var) -> str          code for a FIRST./LAST. boolean column
        lag(arg: Code) -> str                  code for LAG(arg) in the step's window
    """

    def __init__(self, ctx):
        self.ctx = ctx
        self.notes: list[str] = []

    # -- entry points -------------------------------------------------------

    def value(self, e: ir.Expr) -> Code:
        """Expression as a value to store in a column (booleans become 1/0)."""
        c = self.emit(e)
        if c.kind == "bool":
            return Code(f"{c.src}.cast('int')", "num")
        return c

    def column(self, e: ir.Expr) -> str:
        """Expression as a Column (literals wrapped in F.lit)."""
        c = self.value(e)
        return f"F.lit({c.src})" if c.literal else c.src

    def cond(self, e: ir.Expr) -> str:
        """Expression as a non-NULL boolean Column, using SAS truth rules."""
        return self.truth(self.emit(e))

    # -- internals ----------------------------------------------------------

    def truth(self, c: Code) -> str:
        if c.kind == "bool":
            return c.src
        if c.kind == "num":
            if c.literal:
                return f"F.lit({c.src} != 0)"
            return f"F.coalesce({c.src} != 0, F.lit(False))"
        raise Unsupported(f"character value used as a condition: {c.src}")

    def col(self, c: Code) -> str:
        return f"F.lit({c.src})" if c.literal else c.src

    def emit(self, e: ir.Expr) -> Code:
        if isinstance(e, ir.Num):
            return Code(fmt_num(e.value), "num", literal=True)
        if isinstance(e, ir.Str):
            return Code(repr(e.value), "char", literal=True)
        if isinstance(e, ir.Date):
            return Code(py_literal(e), "date", literal=True)
        if isinstance(e, ir.Missing):
            return Code('F.lit(None).cast("double")', "num")
        if isinstance(e, ir.Var):
            if e.name not in self.ctx.schema:
                raise Unsupported(f"variable {e.name} is not defined at this point")
            return Code(f'F.col("{e.name}")', kind_of(self.ctx.schema[e.name]))
        if isinstance(e, ir.FirstLast):
            return Code(self.ctx.first_last(e.which, e.var), "bool")
        if isinstance(e, ir.Unary):
            return self.unary(e)
        if isinstance(e, ir.Binary):
            return self.binary(e)
        if isinstance(e, ir.In):
            c = self.emit(e.operand)
            items = ", ".join(self.emit(i).src for i in e.items)
            return Code(f"({c.src}.isNotNull() & {c.src}.isin({items}))", "bool")
        if isinstance(e, ir.Call):
            return self.call(e)
        raise Unsupported(f"expression {e}")

    def unary(self, e: ir.Unary) -> Code:
        c = self.emit(e.operand)
        if e.op == "not":
            return Code(f"~{self.truth(c)}" if c.kind == "bool" else f"~({self.truth(c)})", "bool")
        if c.literal:
            return Code(f"{e.op}{c.src}", c.kind, literal=True)
        return Code(f"({e.op}{c.src})", c.kind)

    def binary(self, e: ir.Binary) -> Code:
        if e.op in ("and", "or"):
            op = "&" if e.op == "and" else "|"
            return Code(f"({self.cond(e.left)} {op} {self.cond(e.right)})", "bool")
        if e.op in _CMP or e.op in ("=", "ne"):
            return self.compare(e.op, e.left, e.right)

        a, b = self.emit(e.left), self.emit(e.right)
        if a.literal and b.literal:
            return Code(f"({a.src} {e.op if e.op != '**' else '**'} {b.src})", "num", literal=True)
        if e.op == "**":
            return Code(f"F.pow({self.col(a)}, {self.col(b)})", "num")
        if e.op == "/" and not (b.literal and float(b.src) != 0):
            return Code(f"sas.div({self.col(a)}, {self.col(b)})", "num")
        left = self.col(a) if a.literal else a.src      # keep a Column on the left
        return Code(f"({left} {e.op} {b.src})", "num")

    def compare(self, op: str, left: ir.Expr, right: ir.Expr) -> Code:
        # literal on the left: flip so the column is on the left
        if isinstance(left, ir.LITERALS) and not isinstance(right, ir.LITERALS):
            return self.compare(_FLIP[op], right, left)
        a = self.emit(left)

        if isinstance(right, ir.Missing):
            # x = .   x ne .   x > .   x >= .   x < .   x <= .
            return Code({
                "=": f"{a.src}.isNull()", "ne": f"{a.src}.isNotNull()",
                ">": f"{a.src}.isNotNull()", ">=": "F.lit(True)",
                "<": "F.lit(False)", "<=": f"{a.src}.isNull()",
            }[op], "bool")

        b = self.emit(right)
        if op == "=":
            return Code(f"{self.col(a)}.eqNullSafe({b.src})", "bool")
        if op == "ne":
            return Code(f"~{self.col(a)}.eqNullSafe({b.src})", "bool")
        if b.literal and not a.literal:
            # missing sorts first: it is below any literal, never above it
            if op in ("<", "<="):
                name = left.name if isinstance(left, ir.Var) else "expression"
                self.notes.append(f"`{name} {op} {b.src}`: SAS treats missing as smaller than any "
                                  "number, so a NULL satisfies it (plain Spark would say NULL -> false)")
                return Code(f"({a.src}.isNull() | ({a.src} {op} {b.src}))", "bool")
            return Code(f"({a.src}.isNotNull() & ({a.src} {op} {b.src}))", "bool")
        return Code(f"sas.{_CMP[op]}({self.col(a)}, {self.col(b)})", "bool")

    def call(self, e: ir.Call) -> Code:
        name, args = e.name, e.args
        if is_lag(e):
            if len(args) != 1:
                raise Unsupported(f"{name} with {len(args)} arguments")
            arg = self.emit(args[0])
            lagged = self.ctx.lag(arg)
            if name.startswith("dif"):
                return Code(f"({self.col(arg)} - {lagged})", "num")
            return Code(lagged, arg.kind)
        cols = [self.column(a) for a in args]
        if name == "round":
            return Code(f"sas.round_({', '.join(cols[:1] + [self.emit(a).src for a in args[1:]])})", "num")
        if name in ("max", "min"):
            # F.greatest / F.least skip NULLs - exactly SAS MAX / MIN
            return Code(f"F.{'greatest' if name == 'max' else 'least'}({', '.join(cols)})", "num")
        if name == "sum":
            return Code(f"sas.sum_({', '.join(cols)})", "num")
        if name == "abs":
            return Code(f"F.abs({cols[0]})", "num")
        if name in ("year", "month", "day"):
            return Code(f"F.{'dayofmonth' if name == 'day' else name}({cols[0]})", "num")
        if name == "mdy":
            return Code(f"F.make_date({cols[2]}, {cols[0]}, {cols[1]})", "date")
        raise Unsupported(f"function {name}()")


class PyEmitter:
    """SAS expression -> Python over a `pdv` dict (the program data vector).
    first/last flags are local booleans named first_<var> / last_<var>."""

    def emit(self, e: ir.Expr) -> str:
        if isinstance(e, ir.LITERALS):
            return py_literal(e)
        if isinstance(e, ir.Missing):
            return "math.nan"
        if isinstance(e, ir.Var):
            return f'pdv["{e.name}"]'
        if isinstance(e, ir.FirstLast):
            return f"{e.which}_{e.var}"
        if isinstance(e, ir.Unary):
            inner = self.emit(e.operand)
            return f"(not {self.truth(e.operand)})" if e.op == "not" else f"({e.op}{inner})"
        if isinstance(e, ir.Binary):
            if e.op in ("and", "or"):
                return f"({self.truth(e.left)} {e.op} {self.truth(e.right)})"
            a, b = self.emit(e.left), self.emit(e.right)
            cmp = {"=": "eq", "ne": "ne", **_CMP}.get(e.op)
            if cmp:
                return f"sas.py_{cmp}({a}, {b})"
            if e.op == "/":
                return f"sas.py_div({a}, {b})"
            return f"({a} {e.op} {b})"
        if isinstance(e, ir.In):
            return f"({self.emit(e.operand)} in ({', '.join(self.emit(i) for i in e.items)},))"
        if isinstance(e, ir.Call):
            fn = {"round": "sas.py_round", "max": "sas.py_max", "min": "sas.py_min",
                  "sum": "sas.py_sum", "abs": "abs"}.get(e.name)
            if fn is None:
                raise Unsupported(f"function {e.name}() in a sequential step")
            return f"{fn}({', '.join(self.emit(a) for a in e.args)})"
        raise Unsupported(f"expression {e}")

    def truth(self, e: ir.Expr) -> str:
        if isinstance(e, ir.FirstLast) or (isinstance(e, ir.Binary) and e.op in (
                "and", "or", "=", "ne", "<", "<=", ">", ">=")) or isinstance(e, ir.In) \
                or (isinstance(e, ir.Unary) and e.op == "not"):
            return self.emit(e)
        return f"sas.py_true({self.emit(e)})"
