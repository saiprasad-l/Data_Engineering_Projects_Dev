"""Convert SAS programs to PySpark modules in generated/.

    python -m src.converter sas/02_state_running_total.sas
    python -m src.converter --all
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .. import load

STATUS_MARK = {"converted": "✔", "sequential": "✔", "unsupported": "✘", "skipped": "-"}


def main() -> int:
    ap = argparse.ArgumentParser(description="SAS -> PySpark converter")
    ap.add_argument("files", nargs="*", type=Path, help="SAS programs under sas/")
    ap.add_argument("--all", action="store_true", help="convert every pattern program")
    args = ap.parse_args()

    by_file = {p.sas_file: pid for pid, p in load.PATTERNS.items()}
    patterns = list(load.PATTERNS) if args.all else []
    for f in args.files:
        if f.name not in by_file:
            ap.error(f"{f} is not one of the pattern programs: {sorted(by_file)}")
        patterns.append(by_file[f.name])
    if not patterns:
        ap.error("give SAS files or --all")

    failed = 0
    for pid in patterns:
        result = load.convert(pid)
        print(f"\nsas/{load.PATTERNS[pid].sas_file} -> {load.generated_path(pid).relative_to(load.ROOT)}")
        for r in result.reports:
            print(f"  {STATUS_MARK[r.status]} [{r.status}] {r.title}  (lines {r.lines[0]}-{r.lines[1]})")
            for n in r.notes:
                print(f"      - {n}")
        failed += not result.ok
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
