"""SAS -> PySpark converter: parse a .sas file, recognise known patterns, emit PySpark.

    from src.converter import convert
    result = convert(sas_text, "sas/02_state_running_total.sas", {"loans": {...schema...}})
    result.code      # Python module source
    result.reports   # per-step: what was translated how, warnings, unsupported
"""
from .generate import Conversion, StepReport, generate
from .parser import ParseError, parse


def convert(sas_text: str, source_name: str, input_schemas: dict[str, dict[str, str]]) -> Conversion:
    return generate(parse(sas_text), source_name, input_schemas)


__all__ = ["convert", "Conversion", "StepReport", "ParseError"]
