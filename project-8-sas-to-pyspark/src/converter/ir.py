"""What the parser produces: a SAS program as plain data.

Names (variables, datasets) are lower-cased - SAS is case-insensitive.
String literals keep their case.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

# --------------------------------------------------------------------------
# Expressions
# --------------------------------------------------------------------------


class Expr:
    pass


@dataclass
class Num(Expr):
    value: float


@dataclass
class Str(Expr):
    value: str


@dataclass
class Date(Expr):
    value: dt.date


@dataclass
class Missing(Expr):
    """The numeric missing value `.`"""


@dataclass
class Var(Expr):
    name: str


@dataclass
class FirstLast(Expr):
    which: str      # "first" | "last"
    var: str


@dataclass
class Unary(Expr):
    op: str         # "-" | "+" | "not"
    operand: Expr


@dataclass
class Binary(Expr):
    op: str         # + - * / ** | = ne < <= > >= | and or
    left: Expr
    right: Expr


@dataclass
class In(Expr):
    operand: Expr
    items: list[Expr]


@dataclass
class Call(Expr):
    name: str
    args: list[Expr]


LITERALS = (Num, Str, Date)

# --------------------------------------------------------------------------
# Dataset references
# --------------------------------------------------------------------------


@dataclass
class DsOptions:
    in_: str | None = None
    where: Expr | None = None
    keep: list[str] | None = None
    drop: list[str] | None = None
    rename: dict[str, str] = field(default_factory=dict)


@dataclass
class DsRef:
    name: str                       # dataset name, library stripped
    lib: str = "work"
    options: DsOptions = field(default_factory=DsOptions)

    def __str__(self):
        return f"{self.lib}.{self.name}"


# --------------------------------------------------------------------------
# DATA step statements
# --------------------------------------------------------------------------


@dataclass
class Stmt:
    line: int


@dataclass
class Assign(Stmt):
    target: str
    expr: Expr


@dataclass
class SumStmt(Stmt):
    """`target + expr;` - implies RETAIN, treats missing as 0."""
    target: str
    expr: Expr


@dataclass
class If(Stmt):
    cond: Expr
    then: list[Stmt]
    else_: list[Stmt] | None = None


@dataclass
class SubsetIf(Stmt):
    """`if cond;` - rows failing cond stop here and are not output."""
    cond: Expr


@dataclass
class Block(Stmt):
    """`do; ... end;`"""
    body: list[Stmt]


@dataclass
class DoLoop(Stmt):
    var: str
    start: Expr
    stop: Expr
    by: Expr | None
    body: list[Stmt]


@dataclass
class Output(Stmt):
    dataset: str | None = None


@dataclass
class Delete(Stmt):
    pass


@dataclass
class Retain(Stmt):
    items: list[tuple[str, Expr | None]]


@dataclass
class Length(Stmt):
    items: list[tuple[str, bool, int]]      # name, is_char, length


@dataclass
class Keep(Stmt):
    names: list[str]


@dataclass
class Drop(Stmt):
    names: list[str]


@dataclass
class By(Stmt):
    vars: list[str]


@dataclass
class Set(Stmt):
    datasets: list[DsRef]


@dataclass
class Merge(Stmt):
    datasets: list[DsRef]


@dataclass
class Input(Stmt):
    vars: list[tuple[str, bool]]            # name, is_char


@dataclass
class Datalines(Stmt):
    lines: list[str]


@dataclass
class Other(Stmt):
    """A statement the converter does not translate. `ignorable` ones
    (FORMAT, LABEL, ...) have no effect on the data."""
    keyword: str
    text: str
    ignorable: bool


# --------------------------------------------------------------------------
# Steps
# --------------------------------------------------------------------------


@dataclass
class Step:
    line_start: int
    line_end: int
    source: str                     # original SAS text of the step


@dataclass
class DataStep(Step):
    outputs: list[DsRef]
    stmts: list[Stmt]


@dataclass
class ProcSort(Step):
    data: DsRef
    out: DsRef | None
    by: list[str]
    options: list[str]


@dataclass
class MeansOutput:
    out: DsRef
    stats: list[tuple[str, list[str] | None, list[str]]]    # stat, vars (None = VAR list), names


@dataclass
class ProcMeans(Step):
    proc: str                       # "means" | "summary"
    data: DsRef
    options: list[str]
    class_: list[str]
    var: list[str]
    outputs: list[MeansOutput]
    unsupported: list[str]


@dataclass
class UnknownStep(Step):
    kind: str


@dataclass
class Program:
    steps: list[Step]
    globals: list[str]              # statements outside any step (OPTIONS, %LET, ...)
