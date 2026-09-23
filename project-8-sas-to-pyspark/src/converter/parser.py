"""SAS source -> ir.Program.

Covers the subset of SAS the converter translates: DATA steps (SET, MERGE,
BY, RETAIN, IF/ELSE, DO, sum statements, LAG/DIF, DATALINES), PROC SORT and
PROC MEANS/SUMMARY. Anything else is parsed far enough to be reported, not
guessed at.

Stages: strip comments -> lift DATALINES blocks out -> tokenize -> split into
statements on `;` -> group statements into steps -> parse each step.
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass

from . import ir


class ParseError(Exception):
    pass


# --------------------------------------------------------------------------
# Lexing
# --------------------------------------------------------------------------


@dataclass
class Tok:
    kind: str       # name | num | str | date | op | missing
    value: str
    line: int

    def is_(self, *words: str) -> bool:
        return self.kind == "name" and self.value in words

    def __repr__(self):
        return f"{self.value!r}"


_TOKEN = re.compile(r"""
    (?P<ws>\s+)
  | (?P<str>'(?:[^']|'')*'|"(?:[^"]|"")*")(?P<suffix>dt|d|t|n)?(?![A-Za-z0-9_])
  | (?P<num>(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)
  | (?P<name>[A-Za-z_&%][A-Za-z0-9_&]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)
  | (?P<missing>\.)
  | (?P<op>\*\*|<=|>=|\^=|~=|¬=|<>|><|\|\||[-+*/=<>(),;$&|^~¬:#@?!%])
""", re.VERBOSE | re.IGNORECASE)

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


def strip_comments(text: str) -> str:
    """Blank out /* */ comments, keeping newlines so line numbers survive.
    Statement comments (`* ... ;`) are dropped later, at statement level."""
    out, i = [], 0
    in_str = None
    while i < len(text):
        c = text[i]
        if in_str:
            out.append(c)
            if c == in_str:
                in_str = None
        elif c in "'\"":
            in_str = c
            out.append(c)
        elif text.startswith("/*", i):
            end = text.find("*/", i + 2)
            end = len(text) if end < 0 else end + 2
            out.append(re.sub(r"[^\n]", " ", text[i:end]))
            i = end
            continue
        else:
            out.append(c)
        i += 1
    return "".join(out)


_DATALINES = re.compile(r"\b(datalines|cards|lines)\s*;[^\n]*\n(.*?)^\s*;\s*$", re.IGNORECASE | re.DOTALL | re.MULTILINE)


def lift_datalines(text: str) -> tuple[str, dict[str, list[str]]]:
    """Replace each in-stream data block with a placeholder statement.
    Its lines are blanked (not removed) so later line numbers stay right."""
    blocks: dict[str, list[str]] = {}

    def repl(m):
        key = f"__datalines{len(blocks)}__"
        body = m.group(2)
        blocks[key] = [ln for ln in body.splitlines() if ln.strip()]
        return f"datalines {key};" + "\n" * (m.group(0).count("\n"))

    return _DATALINES.sub(repl, text), blocks


def tokenize(text: str) -> list[Tok]:
    toks, line, pos = [], 1, 0
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise ParseError(f"line {line}: unexpected character {text[pos]!r}")
        kind = m.lastgroup if m.lastgroup != "suffix" else "str"
        s = m.group(0)
        if m.group("ws") is None:
            if m.group("str") is not None:
                raw = m.group("str")
                quote = raw[0]
                value = raw[1:-1].replace(quote * 2, quote)
                suffix = (m.group("suffix") or "").lower()
                if suffix == "d":
                    toks.append(Tok("date", value, line))
                elif suffix:
                    raise ParseError(f"line {line}: {suffix!r} literals are not supported")
                else:
                    toks.append(Tok("str", value, line))
            elif m.group("num") is not None:
                toks.append(Tok("num", s, line))
            elif m.group("name") is not None:
                toks.append(Tok("name", s.lower(), line))
            elif m.group("missing") is not None:
                toks.append(Tok("missing", ".", line))
            else:
                toks.append(Tok("op", s, line))
        line += s.count("\n")
        pos = m.end()
    return toks


def parse_date(text: str, line: int) -> dt.date:
    m = re.fullmatch(r"(\d{1,2})([A-Za-z]{3})(\d{2}|\d{4})", text.strip())
    if not m:
        raise ParseError(f"line {line}: bad date literal {text!r}")
    day, mon, year = int(m.group(1)), _MONTHS[m.group(2).lower()], int(m.group(3))
    if year < 100:
        year += 1900 if year >= 60 else 2000
    return dt.date(year, mon, day)


# --------------------------------------------------------------------------
# Expressions (precedence climbing, SAS operator groups)
# --------------------------------------------------------------------------

_COMPARE = {"=": "=", "eq": "=", "^=": "ne", "~=": "ne", "¬=": "ne", "ne": "ne",
            "<": "<", "lt": "<", "<=": "<=", "le": "<=", ">": ">", "gt": ">",
            ">=": ">=", "ge": ">="}


class ExprParser:
    def __init__(self, toks: list[Tok]):
        self.toks = toks
        self.i = 0

    def peek(self) -> Tok | None:
        return self.toks[self.i] if self.i < len(self.toks) else None

    def next(self) -> Tok:
        t = self.peek()
        if t is None:
            raise ParseError("unexpected end of expression")
        self.i += 1
        return t

    def expect(self, value: str):
        t = self.next()
        if t.value != value:
            raise ParseError(f"line {t.line}: expected {value!r}, got {t.value!r}")

    def parse(self) -> ir.Expr:
        e = self.or_()
        if self.peek() is not None:
            t = self.peek()
            raise ParseError(f"line {t.line}: unexpected {t.value!r} in expression")
        return e

    def _at(self, *values) -> bool:
        t = self.peek()
        return t is not None and (t.value in values) and t.kind in ("op", "name")

    def or_(self):
        e = self.and_()
        while self._at("or", "|"):
            self.next()
            e = ir.Binary("or", e, self.and_())
        return e

    def and_(self):
        e = self.compare()
        while self._at("and", "&"):
            self.next()
            e = ir.Binary("and", e, self.compare())
        return e

    def compare(self):
        e = self.additive()
        while True:
            t = self.peek()
            if t is None:
                return e
            if t.kind in ("op", "name") and t.value in _COMPARE:
                self.next()
                e = ir.Binary(_COMPARE[t.value], e, self.additive())
            elif t.is_("in") or (t.is_("not") and self.i + 1 < len(self.toks) and self.toks[self.i + 1].is_("in")):
                negate = t.is_("not")
                if negate:
                    self.next()
                self.next()
                self.expect("(")
                items = [self.additive()]
                while self._at(","):
                    self.next()
                    items.append(self.additive())
                self.expect(")")
                e = ir.In(e, items)
                if negate:
                    e = ir.Unary("not", e)
            else:
                return e

    def additive(self):
        e = self.multiplicative()
        while self._at("+", "-"):
            op = self.next().value
            e = ir.Binary(op, e, self.multiplicative())
        return e

    def multiplicative(self):
        e = self.unary()
        while self._at("*", "/"):
            op = self.next().value
            e = ir.Binary(op, e, self.unary())
        return e

    def unary(self):
        if self._at("-", "+"):
            op = self.next().value
            return ir.Unary(op, self.unary())
        if self._at("not", "^", "~", "¬"):
            self.next()
            return ir.Unary("not", self.unary())
        e = self.atom()
        if self._at("**"):
            self.next()
            e = ir.Binary("**", e, self.unary())
        return e

    def atom(self):
        t = self.next()
        if t.kind == "num":
            return ir.Num(float(t.value))
        if t.kind == "str":
            return ir.Str(t.value)
        if t.kind == "date":
            return ir.Date(parse_date(t.value, t.line))
        if t.kind == "missing":
            return ir.Missing()
        if t.value == "(":
            e = self.or_()
            self.expect(")")
            return e
        if t.kind == "name":
            if t.value.startswith(("&", "%")):
                raise ParseError(f"line {t.line}: macro reference {t.value} not supported")
            if t.value.startswith(("first.", "last.")):
                which, var = t.value.split(".", 1)
                return ir.FirstLast(which, var)
            if self._at("("):
                self.next()
                args = []
                if not self._at(")"):
                    args.append(self.or_())
                    while self._at(","):
                        self.next()
                        args.append(self.or_())
                self.expect(")")
                return ir.Call(t.value, args)
            return ir.Var(t.value)
        raise ParseError(f"line {t.line}: unexpected {t.value!r}")


def parse_expr(toks: list[Tok]) -> ir.Expr:
    if not toks:
        raise ParseError("empty expression")
    return ExprParser(toks).parse()


# --------------------------------------------------------------------------
# Statements
# --------------------------------------------------------------------------


@dataclass
class RawStmt:
    toks: list[Tok]
    line: int
    text: str

    @property
    def head(self) -> str:
        return self.toks[0].value if self.toks else ""


def split_statements(toks: list[Tok], text_lines: list[str]) -> list[RawStmt]:
    stmts, cur = [], []
    for t in toks:
        if t.kind == "op" and t.value == ";":
            if cur:
                if not (cur[0].kind == "op" and cur[0].value == "*"):    # `* comment;`
                    first, last = cur[0].line, cur[-1].line
                    stmts.append(RawStmt(cur, first, "\n".join(text_lines[first - 1:last]).strip()))
            cur = []
        else:
            cur.append(t)
    if cur:
        raise ParseError(f"line {cur[0].line}: statement not terminated by ';'")
    return stmts


def _split_top(toks: list[Tok], value: str) -> int:
    """Index of the first `value` token at paren depth 0, or -1."""
    depth = 0
    for i, t in enumerate(toks):
        if t.value == "(":
            depth += 1
        elif t.value == ")":
            depth -= 1
        elif depth == 0 and t.value == value and t.kind in ("name", "op"):
            return i
    return -1


def _names(toks: list[Tok]) -> list[str]:
    bad = [t for t in toks if t.kind != "name"]
    if bad:
        raise ParseError(f"line {bad[0].line}: expected variable names, got {bad[0].value!r}")
    return [t.value for t in toks]


def _group(toks: list[Tok], i: int) -> tuple[list[Tok], int]:
    """toks[i] is '(' - return the tokens inside the matching ')' and the index after it."""
    depth, j = 0, i
    while j < len(toks):
        if toks[j].value == "(":
            depth += 1
        elif toks[j].value == ")":
            depth -= 1
            if depth == 0:
                return toks[i + 1:j], j + 1
        j += 1
    raise ParseError(f"line {toks[i].line}: unbalanced parentheses")


def parse_dsref(toks: list[Tok], i: int) -> tuple[ir.DsRef, int]:
    t = toks[i]
    if t.kind != "name":
        raise ParseError(f"line {t.line}: expected dataset name, got {t.value!r}")
    lib, _, name = t.value.rpartition(".")
    ref = ir.DsRef(name=name, lib=lib or "work")
    i += 1
    if i < len(toks) and toks[i].value == "(":
        inner, i = _group(toks, i)
        ref.options = parse_ds_options(inner)
    return ref, i


def parse_ds_options(toks: list[Tok]) -> ir.DsOptions:
    opts = ir.DsOptions()
    i = 0
    while i < len(toks):
        key = toks[i]
        if key.kind != "name" or i + 1 >= len(toks) or toks[i + 1].value != "=":
            raise ParseError(f"line {key.line}: bad dataset option near {key.value!r}")
        i += 2
        if key.value in ("where", "rename"):
            inner, i = _group(toks, i)
            if key.value == "where":
                opts.where = parse_expr(inner)
            else:
                for j in range(0, len(inner), 3):
                    if j + 2 >= len(inner) or inner[j + 1].value != "=":
                        raise ParseError(f"line {key.line}: bad rename=")
                    opts.rename[inner[j].value] = inner[j + 2].value
        elif key.value == "in":
            opts.in_ = toks[i].value
            i += 1
        elif key.value in ("keep", "drop"):
            names = []
            while i < len(toks) and not (i + 1 < len(toks) and toks[i + 1].value == "="):
                names.append(toks[i].value)
                i += 1
            setattr(opts, key.value, names)
        else:
            raise ParseError(f"line {key.line}: dataset option {key.value}= not supported")
    return opts


_IGNORABLE = {"format", "label", "informat", "attrib", "title", "footnote"}


class StmtParser:
    """Turns a DATA step's raw statements into nested ir.Stmt."""

    def __init__(self, raws: list[RawStmt], datalines: dict[str, list[str]]):
        self.raws = raws
        self.i = 0
        self.datalines = datalines

    def block(self, until_end: bool) -> list[ir.Stmt]:
        out = []
        while self.i < len(self.raws):
            raw = self.raws[self.i]
            if raw.head == "end":
                if not until_end:
                    raise ParseError(f"line {raw.line}: END without DO")
                self.i += 1
                return out
            self.i += 1
            out.append(self.stmt(raw.toks, raw))
        if until_end:
            raise ParseError("DO block not closed by END")
        return out

    def body_of(self, toks: list[Tok], raw: RawStmt) -> list[ir.Stmt]:
        """Statement(s) after THEN / ELSE: either `do; ... end;` or one statement."""
        if len(toks) == 1 and toks[0].is_("do"):
            return self.block(until_end=True)
        return [self.stmt(toks, raw)]

    def stmt(self, toks: list[Tok], raw: RawStmt) -> ir.Stmt:
        line = toks[0].line
        head = toks[0].value if toks[0].kind == "name" else ""

        # assignment / sum statement first: `x = ...`, `x + ...` (x may shadow a keyword)
        if toks[0].kind == "name" and len(toks) > 1 and toks[1].value == "=" and head not in ("if",):
            return ir.Assign(line, toks[0].value, parse_expr(toks[2:]))
        if toks[0].kind == "name" and len(toks) > 1 and toks[1].value == "+" and _split_top(toks, "=") < 0 \
                and head not in ("if", "else", "do", "output", "delete", "retain", "keep", "drop"):
            return ir.SumStmt(line, toks[0].value, parse_expr(toks[2:]))

        if head == "if":
            k = _split_top(toks, "then")
            if k < 0:
                return ir.SubsetIf(line, parse_expr(toks[1:]))
            node = ir.If(line, parse_expr(toks[1:k]), self.body_of(toks[k + 1:], raw))
            if self.i < len(self.raws) and self.raws[self.i].head == "else":
                nxt = self.raws[self.i]
                self.i += 1
                node.else_ = self.body_of(nxt.toks[1:], nxt)
            return node
        if head == "else":
            raise ParseError(f"line {line}: ELSE without IF")
        if head == "do":
            if len(toks) == 1:
                return ir.Block(line, self.block(until_end=True))
            if len(toks) > 2 and toks[2].value == "=":
                to = _split_top(toks, "to")
                if to < 0:
                    raise ParseError(f"line {line}: only DO var = start TO stop [BY step] is supported")
                by = _split_top(toks, "by")
                stop_end = by if by > 0 else len(toks)
                return ir.DoLoop(line, toks[1].value, parse_expr(toks[3:to]),
                                 parse_expr(toks[to + 1:stop_end]),
                                 parse_expr(toks[by + 1:]) if by > 0 else None,
                                 self.block(until_end=True))
            # DO WHILE / DO UNTIL: keep the body balanced, report as unsupported
            self.block(until_end=True)
            return ir.Other(line, "do " + toks[1].value, raw.text, ignorable=False)
        if head in ("set", "merge"):
            refs, i = [], 1
            while i < len(toks):
                ref, i = parse_dsref(toks, i)
                refs.append(ref)
            return ir.Set(line, refs) if head == "set" else ir.Merge(line, refs)
        if head == "by":
            if any(t.is_("descending", "notsorted") for t in toks[1:]):
                return ir.Other(line, "by", raw.text, ignorable=False)
            return ir.By(line, _names(toks[1:]))
        if head == "retain":
            items, pending = [], []
            for t in toks[1:]:
                if t.kind == "name":
                    pending.append(t.value)
                else:
                    init = {"num": lambda v: ir.Num(float(v)), "str": ir.Str,
                            "missing": lambda v: ir.Missing()}.get(t.kind)
                    if init is None:
                        raise ParseError(f"line {line}: bad RETAIN initial value {t.value!r}")
                    items += [(n, init(t.value)) for n in pending]
                    pending = []
            items += [(n, None) for n in pending]
            if not items:
                return ir.Other(line, "retain", raw.text, ignorable=False)
            return ir.Retain(line, items)
        if head == "length":
            items, pending, j = [], [], 1
            while j < len(toks):
                t = toks[j]
                if t.kind == "name":
                    pending.append(t.value)
                    j += 1
                elif t.value == "$":
                    n = int(float(toks[j + 1].value))
                    items += [(p, True, n) for p in pending]
                    pending, j = [], j + 2
                elif t.kind == "num":
                    items += [(p, False, int(float(t.value))) for p in pending]
                    pending, j = [], j + 1
                else:
                    raise ParseError(f"line {line}: bad LENGTH near {t.value!r}")
            return ir.Length(line, items)
        if head in ("keep", "drop"):
            return (ir.Keep if head == "keep" else ir.Drop)(line, _names(toks[1:]))
        if head == "output":
            return ir.Output(line, toks[1].value.rpartition(".")[2] if len(toks) > 1 else None)
        if head == "delete":
            return ir.Delete(line)
        if head == "input":
            vars_, j = [], 1
            while j < len(toks):
                if toks[j].kind != "name":
                    raise ParseError(f"line {line}: only list INPUT (name [$]) is supported")
                is_char = j + 1 < len(toks) and toks[j + 1].value == "$"
                vars_.append((toks[j].value, is_char))
                j += 2 if is_char else 1
            return ir.Input(line, vars_)
        if head == "datalines":
            return ir.Datalines(line, self.datalines[toks[1].value])
        return ir.Other(line, head or toks[0].value, raw.text, ignorable=head in _IGNORABLE)


# --------------------------------------------------------------------------
# Steps
# --------------------------------------------------------------------------


def _proc_options(toks: list[Tok]) -> tuple[dict[str, ir.DsRef], list[str]]:
    """`proc x data=a out=b nway noprint` -> ({'data': a, 'out': b}, ['nway', 'noprint'])"""
    refs, flags, i = {}, [], 0
    while i < len(toks):
        t = toks[i]
        if i + 1 < len(toks) and toks[i + 1].value == "=":
            ref, i = parse_dsref(toks, i + 2)
            refs[t.value] = ref
        else:
            flags.append(t.value)
            i += 1
    return refs, flags


def parse_proc_sort(raws: list[RawStmt], span) -> ir.Step:
    refs, flags = _proc_options(raws[0].toks[2:])
    by = []
    for r in raws[1:]:
        if r.head == "by":
            if any(t.is_("descending") for t in r.toks[1:]):
                return ir.UnknownStep(*span, kind="proc sort (BY DESCENDING)")
            by = _names(r.toks[1:])
        else:
            return ir.UnknownStep(*span, kind=f"proc sort ({r.head} statement)")
    return ir.ProcSort(*span, data=refs["data"], out=refs.get("out"), by=by, options=flags)


_STATS = {"n", "nmiss", "sum", "mean", "min", "max", "std", "var"}


def parse_proc_means(raws: list[RawStmt], span) -> ir.Step:
    proc = raws[0].toks[1].value
    refs, flags = _proc_options(raws[0].toks[2:])
    step = ir.ProcMeans(*span, proc=proc, data=refs["data"], options=flags,
                        class_=[], var=[], outputs=[], unsupported=[])
    for r in raws[1:]:
        if r.head == "class":
            if _split_top(r.toks, "/") >= 0:
                step.unsupported.append("CLASS options")
            step.class_ = _names(r.toks[1:])
        elif r.head == "var":
            step.var = _names(r.toks[1:])
        elif r.head == "output":
            toks = r.toks[1:]
            if not (toks and toks[0].is_("out") and toks[1].value == "="):
                raise ParseError(f"line {r.line}: OUTPUT needs OUT=")
            out, i = parse_dsref(toks, 2)
            stats = []
            while i < len(toks):
                stat = toks[i].value
                if stat == "/":
                    step.unsupported.append("OUTPUT options (e.g. /AUTONAME)")
                    break
                if stat not in _STATS:
                    step.unsupported.append(f"statistic {stat}")
                i += 1
                vars_ = None
                if i < len(toks) and toks[i].value == "(":
                    inner, i = _group(toks, i)
                    vars_ = _names(inner)
                if i >= len(toks) or toks[i].value != "=":
                    raise ParseError(f"line {r.line}: expected '=' after {stat}")
                i += 1
                names = []
                while i < len(toks) and toks[i].kind == "name" and not (
                        i + 1 < len(toks) and toks[i + 1].value in ("=", "(")):
                    names.append(toks[i].value)
                    i += 1
                stats.append((stat, vars_, names))
            step.outputs.append(ir.MeansOutput(out, stats))
        else:
            step.unsupported.append(f"{r.head.upper()} statement")
    return step


def parse(text: str) -> ir.Program:
    lines = text.splitlines()
    clean, datalines = lift_datalines(strip_comments(text))
    raws = split_statements(tokenize(clean), lines)

    steps: list[ir.Step] = []
    globals_: list[str] = []
    i = 0
    while i < len(raws):
        head = raws[i].head
        if head not in ("data", "proc"):
            globals_.append(raws[i].text)
            i += 1
            continue
        j = i + 1
        while j < len(raws) and raws[j].head not in ("run", "quit", "data", "proc"):
            j += 1
        body = raws[i:j]
        end_line = raws[j].line if j < len(raws) and raws[j].head in ("run", "quit") else body[-1].line
        span = (raws[i].line, end_line, "\n".join(lines[raws[i].line - 1:end_line]))
        steps.append(_parse_step(body, datalines, span))
        i = j + 1 if j < len(raws) and raws[j].head in ("run", "quit") else j
    return ir.Program(steps, globals_)


def _parse_step(body: list[RawStmt], datalines, span) -> ir.Step:
    first = body[0]
    if first.head == "data":
        outputs, i = [], 1
        while i < len(first.toks):
            ref, i = parse_dsref(first.toks, i)
            if ref.name != "_null_":
                outputs.append(ref)
        stmts = StmtParser(body[1:], datalines).block(until_end=False)
        return ir.DataStep(*span, outputs=outputs, stmts=stmts)
    proc = first.toks[1].value if len(first.toks) > 1 else "?"
    if proc == "sort":
        return parse_proc_sort(body, span)
    if proc in ("means", "summary"):
        return parse_proc_means(body, span)
    return ir.UnknownStep(*span, kind=f"proc {proc}")
