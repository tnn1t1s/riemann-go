"""Score a harness run.

Categories classify a failure. They never gate a pass: the matcher's verdict is
the final arbiter, so a scenario whose every assertion held is GREEN even if
no sink was ever posted to, because some scenarios assert exactly that.
"""

from typing import Any, Dict


def compute(
    *,
    compiles: bool,
    starts: bool,
    sink_connected: bool,
    observer_ok: bool,
    predicate_results: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    total = max(len(predicate_results), 1)
    passed = sum(1 for r in predicate_results.values() if r.get("ok"))
    scenario_score = passed / total if predicate_results else 0.0

    if not observer_ok:
        category = "observer_error"
        overall = 0.0
    elif scenario_score == 1.0:
        category = "GREEN"
        overall = 1.0
    elif not compiles:
        category = "compile_error"
        overall = 0.0
    elif not starts:
        category = "start_error"
        overall = 0.0
    elif not sink_connected:
        category = "sink_connect_error"
        overall = 0.0
    else:
        category = "predicate_violation"
        overall = scenario_score

    return {
        "overall": overall,
        "category": category,
        "compile_score": 1.0 if compiles else 0.0,
        "start_score": 1.0 if starts else 0.0,
        "sink_connect_score": 1.0 if sink_connected else 0.0,
        "scenario_score": scenario_score,
        "assertions_passed": passed,
        "assertions_total": total,
    }
