"""The oracle: a plain-Python re-enactment of each SAS program in sas/.

This is what the converter's PySpark output is checked against, standing in
for real SAS output. It is deliberately BORING:

  - every DATA step is a `for` loop over rows in sort order, with the program
    data vector (PDV) as a dict, RETAINed values carried explicitly and
    FIRST./LAST. computed by looking at the neighbouring rows
  - SAS rules are spelled out in small named helpers (missing < any number,
    ROUND half-away-from-zero, LAG as a queue, MERGE BY one-to-many)
  - no vectorised pandas tricks, no window functions

If this module were clever it could be wrong in the same way the migration is
wrong, and parity would pass falsely. Keep it literal.

Representation: numeric missing = NaN, character missing = None/NaN. SAS would
print a blank for a character missing; parity treats both as missing.
"""
from __future__ import annotations

import math

import pandas as pd

# --------------------------------------------------------------------------
# SAS rules
# --------------------------------------------------------------------------


def missing(v) -> bool:
    """SAS missing: numeric '.', or a blank character value."""
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def lt(a, b) -> bool:
    """a < b with SAS ordering: missing sorts below every number."""
    if missing(a) and missing(b):
        return False
    if missing(a):
        return True
    if missing(b):
        return False
    return a < b


def ge(a, b) -> bool:
    """a >= b with SAS ordering."""
    return not lt(a, b)


def gt(a, b) -> bool:
    """a > b with SAS ordering."""
    return lt(b, a)


def add(a, b) -> float:
    """The + operator: missing in, missing out."""
    if missing(a) or missing(b):
        return math.nan
    return a + b


def sum_stmt(acc, incr) -> float:
    """The sum statement `acc + incr;`: missing is treated as 0."""
    return (0 if missing(acc) else acc) + (0 if missing(incr) else incr)


def sas_max(*values) -> float:
    """MAX(): ignores missing arguments."""
    present = [v for v in values if not missing(v)]
    return max(present) if present else math.nan


def sas_round(x, unit: float = 0.01) -> float:
    """ROUND(x, unit): nearest multiple of unit, ties away from zero.

    SAS applies a small fuzz so that a value which is a half in decimal but
    slightly under it in binary (2.675 -> 2.67499999...) still rounds up.
    The 1e-7 fuzz is in units of `unit` (i.e. a hundred-thousandth of a cent),
    far wider than binary error and far narrower than any real difference.
    """
    if missing(x):
        return math.nan
    q = abs(x) / unit
    n = math.floor(q)
    if q - n >= 0.5 - 1e-7:
        n += 1
    return round(math.copysign(n * unit, x), 10)


class Lag:
    """LAG() is a queue, updated only when the call executes.

    Each LAG call site in the source owns one queue. It returns the value that
    was stored on the PREVIOUS execution of that call site (missing at first),
    then stores the current argument.
    """

    def __init__(self):
        self.stored = math.nan

    def __call__(self, value):
        out, self.stored = self.stored, value
        return out


def rows_in_order(df: pd.DataFrame, by: list[str]) -> list[dict]:
    """PROC SORT BY ...: stable sort, missing values first (SAS collating)."""
    return df.sort_values(by, kind="mergesort", na_position="first").to_dict("records")


def first_last(rows: list[dict], by: str) -> list[tuple[dict, bool, bool]]:
    """Attach FIRST.by / LAST.by by comparing each row with its neighbours."""
    out = []
    for i, row in enumerate(rows):
        first = i == 0 or rows[i - 1][by] != row[by]
        last = i == len(rows) - 1 or rows[i + 1][by] != row[by]
        out.append((row, first, last))
    return out


def keep(record: dict, columns: list[str]) -> dict:
    return {c: record[c] for c in columns}


# --------------------------------------------------------------------------
# One function per program in sas/
# --------------------------------------------------------------------------


def p01_risk_tier(loans: pd.DataFrame) -> pd.DataFrame:
    out = []
    for row in loans.to_dict("records"):
        pdv = dict(row)

        if ge(pdv["fico_range_low"], 740) and lt(pdv["dti"], 20):
            pdv["risk_tier"] = "LOW"
        elif ge(pdv["fico_range_low"], 680) and lt(pdv["dti"], 35):
            pdv["risk_tier"] = "MEDIUM"
        else:
            pdv["risk_tier"] = "HIGH"

        if gt(pdv["revol_util"], 90):
            pdv["high_util"] = 1
        else:
            pdv["high_util"] = 0

        pdv["is_bad"] = 1 if pdv["loan_status"] in ("Charged Off", "Default") else 0

        out.append(keep(pdv, ["id", "grade", "fico_range_low", "dti", "revol_util",
                              "loan_status", "risk_tier", "high_util", "is_bad"]))
    return pd.DataFrame(out)


def p02_state_running_total(loans: pd.DataFrame) -> pd.DataFrame:
    rows = rows_in_order(loans, ["addr_state", "issue_d", "id"])
    out = []
    cum_funded = 0          # retain cum_funded 0;
    cum_loans = 0           # sum statement => implicitly retained, starts at 0
    for row, first, _last in first_last(rows, "addr_state"):
        pdv = dict(row)
        if first:
            cum_funded = 0
            cum_loans = 0
        cum_funded = add(cum_funded, pdv["funded_amnt"])
        cum_loans = sum_stmt(cum_loans, 1)
        pdv["cum_funded"], pdv["cum_loans"] = cum_funded, cum_loans
        out.append(keep(pdv, ["addr_state", "issue_d", "id", "funded_amnt",
                              "cum_funded", "cum_loans"]))
    return pd.DataFrame(out)


def p03_grade_sequence(loans: pd.DataFrame) -> pd.DataFrame:
    rows = rows_in_order(loans, ["grade", "issue_d", "id"])
    out = []
    loan_seq = 0            # sum statement => implicitly retained
    for row, first, last in first_last(rows, "grade"):
        pdv = dict(row)
        if first:
            loan_seq = 0
        loan_seq = sum_stmt(loan_seq, 1)
        pdv["loan_seq"] = loan_seq
        pdv["is_first"] = 1 if first else 0
        pdv["is_last"] = 1 if last else 0
        out.append(keep(pdv, ["grade", "issue_d", "id", "loan_seq", "is_first", "is_last"]))
    return pd.DataFrame(out)


def _proc_means_nway(df: pd.DataFrame, class_vars: list[str]) -> dict[tuple, list[dict]]:
    """Group rows for PROC MEANS/SUMMARY NWAY. Rows with a missing CLASS value
    are excluded (no MISSING option). Returned in CLASS sort order."""
    groups: dict[tuple, list[dict]] = {}
    for row in df.to_dict("records"):
        if any(missing(row[c]) for c in class_vars):
            continue
        groups.setdefault(tuple(row[c] for c in class_vars), []).append(row)
    return dict(sorted(groups.items()))


def _stat_values(rows: list[dict], var: str) -> list[float]:
    return [r[var] for r in rows if not missing(r[var])]


def p04_grade_month_change(loans: pd.DataFrame) -> pd.DataFrame:
    # proc summary nway; class grade issue_d; var funded_amnt; sum=funded; _freq_ -> n_loans
    grade_month = []
    for (grade, issue_d), rows in _proc_means_nway(loans, ["grade", "issue_d"]).items():
        vals = _stat_values(rows, "funded_amnt")
        grade_month.append({"grade": grade, "issue_d": issue_d, "n_loans": len(rows),
                            "funded": sum(vals) if vals else math.nan})

    # data step: by grade; lag + dif, blanked on first.grade
    lag_funded = Lag()      # the LAG() call site
    dif_funded = Lag()      # DIF(x) = x - LAG(x), with its own queue
    out = []
    for row, first, _last in first_last(grade_month, "grade"):
        pdv = dict(row)
        pdv["prev_funded"] = lag_funded(pdv["funded"])
        prior = dif_funded(pdv["funded"])
        pdv["funded_chg"] = math.nan if missing(prior) or missing(pdv["funded"]) else pdv["funded"] - prior
        if first:
            pdv["prev_funded"] = math.nan
            pdv["funded_chg"] = math.nan
        out.append(keep(pdv, ["grade", "issue_d", "n_loans", "funded", "prev_funded", "funded_chg"]))
    return pd.DataFrame(out)


def p05_portfolio_summary(loans: pd.DataFrame) -> pd.DataFrame:
    out = []
    for (grade, emp_length), rows in _proc_means_nway(loans, ["grade", "emp_length"]).items():
        funded = _stat_values(rows, "funded_amnt")
        rate = _stat_values(rows, "int_rate")
        dti = _stat_values(rows, "dti")
        out.append({
            "grade": grade,
            "emp_length": emp_length,
            "n_loans": len(rows),                                   # _FREQ_: rows
            "total_funded": sum(funded) if funded else math.nan,
            "avg_rate": sum(rate) / len(rate) if rate else math.nan,
            "min_rate": min(rate) if rate else math.nan,
            "max_rate": max(rate) if rate else math.nan,
            "n_dti": len(dti),                                      # N(dti): non-missing
            "avg_dti": sum(dti) / len(dti) if dti else math.nan,
        })
    return pd.DataFrame(out)


BRANCH_REGION = {
    "CT": "Northeast", "MA": "Northeast", "NJ": "Northeast", "NY": "Northeast",
    "PA": "Northeast", "RI": "Northeast",
    "DE": "MidAtlantic", "MD": "MidAtlantic", "VA": "MidAtlantic", "WV": "MidAtlantic",
    "NC": "Southeast", "SC": "Southeast", "GA": "Southeast", "FL": "Southeast",
    "AL": "Southeast", "TN": "Southeast", "KY": "Southeast", "MS": "Southeast",
    "IL": "Midwest", "IN": "Midwest", "IA": "Midwest", "MI": "Midwest", "MN": "Midwest",
    "MO": "Midwest", "OH": "Midwest", "WI": "Midwest", "KS": "Midwest", "NE": "Midwest",
    "AR": "SouthCentral", "LA": "SouthCentral", "OK": "SouthCentral", "TX": "SouthCentral",
    "AZ": "West", "CA": "West", "CO": "West", "NV": "West", "NM": "West", "OR": "West",
    "UT": "West", "WA": "West",
}


def _merge_by(left: list[dict], right: list[dict], by: str):
    """MERGE left(in=a) right(in=b); BY by;  - the SAS algorithm, not a join.

    Within each BY group the i-th left row pairs with the i-th right row. When
    one side runs out, its last row's values are retained for the rest of the
    group. A side with no rows in the group contributes missing values. For a
    column present on both sides the value read LAST (right) wins.
    Yields (pdv, in_a, in_b).
    """
    def groups(rows):
        g: dict = {}
        for r in rows:
            g.setdefault(r[by], []).append(r)
        return g

    lg, rg = groups(left), groups(right)
    left_cols = list(left[0].keys()) if left else []
    right_cols = list(right[0].keys()) if right else []

    for key in sorted(set(lg) | set(rg)):
        l_rows, r_rows = lg.get(key, []), rg.get(key, [])
        for i in range(max(len(l_rows), len(r_rows))):
            pdv = {c: None for c in left_cols + right_cols}
            if l_rows:
                pdv.update(l_rows[min(i, len(l_rows) - 1)])
            if r_rows:
                pdv.update(r_rows[min(i, len(r_rows) - 1)])
            pdv[by] = key
            yield pdv, bool(l_rows), bool(r_rows)


def p07_branch_region(loans: pd.DataFrame) -> pd.DataFrame:
    branch = [{"addr_state": s, "region": r} for s, r in sorted(BRANCH_REGION.items())]
    left = rows_in_order(loans, ["addr_state"])
    out = []
    for pdv, in_loans, in_ref in _merge_by(left, branch, "addr_state"):
        if not in_loans:            # if in_loans;
            continue
        if not in_ref:
            pdv["region"] = "UNMAPPED"
        out.append(keep(pdv, ["id", "addr_state", "region", "funded_amnt"]))
    return pd.DataFrame(out)


def p10_amortization(loans: pd.DataFrame) -> pd.DataFrame:
    # data work.schedule: where= filter, then DO month_num = 1 TO term; OUTPUT; END;
    schedule = []
    for row in loans.to_dict("records"):
        if not (row["issue_d"] >= "2018-10-01" and row["grade"] == "A"):
            continue
        monthly_rate = row["int_rate"] / 100 / 12
        for month_num in range(1, int(row["term"]) + 1):
            schedule.append({"id": row["id"], "month_num": month_num, "term": row["term"],
                             "monthly_rate": monthly_rate, "installment": row["installment"],
                             "funded_amnt": row["funded_amnt"]})

    # proc sort by id month_num; data work.amortization: by id; retain balance;
    rows = rows_in_order(pd.DataFrame(schedule), ["id", "month_num"])
    out = []
    balance = math.nan
    for row, first, _last in first_last(rows, "id"):
        pdv = dict(row)
        if first:
            balance = pdv["funded_amnt"]
        interest = sas_round(balance * pdv["monthly_rate"], 0.01)
        principal = sas_round(pdv["installment"] - interest, 0.01)
        balance = sas_max(0, sas_round(balance - principal, 0.01))
        pdv.update(interest=interest, principal=principal, balance=balance)
        out.append(keep(pdv, ["id", "month_num", "interest", "principal", "balance"]))
    return pd.DataFrame(out)


PROGRAMS = {
    "01": p01_risk_tier,
    "02": p02_state_running_total,
    "03": p03_grade_sequence,
    "04": p04_grade_month_change,
    "05": p05_portfolio_summary,
    "07": p07_branch_region,
    "10": p10_amortization,
}


def run(pattern: str, df: pd.DataFrame) -> pd.DataFrame:
    """Output of sas/<pattern>_*.sas under SAS semantics."""
    return PROGRAMS[pattern](df)
