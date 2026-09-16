"""Score a harness run.

Categories classify a failure. They never gate a pass: the matcher's verdict is
the final arbiter, so a scenario whose every assertion held is GREEN even if
no sink was ever posted to, because some scenarios assert exactly that.

The three middle categories name riemann-go's own pipeline stages, so a failed
report says how far a generation got before it stopped working.

`rule_error` means events were admitted and no rule ever fired. A firing is
not visible at the sink receiver on its own, because a firing that reaches a
sink is indistinguishable from sink traffic and a firing into the `index` sink
reaches no external process at all. The runner therefore reads each seeded
rule's node counters after settle, which SPEC.md's rule read endpoint returns,
and a rule whose every node reads zero has not fired. That read is harness
evidence rather than oracle evidence, which is acceptable here because it
classifies a failure and no scenario asserts on it.
"""

from typing import Any, Dict


def compute(
    *,
    compiles: bool,
    starts: bool,
    events_admitted: bool,
    rules_registered: bool,
    sink_reached: bool,
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
    elif not events_admitted:
        category = "ingest_error"
        overall = 0.0
    elif not rules_registered:
        category = "rule_error"
        overall = 0.0
    elif not sink_reached:
        category = "sink_error"
        overall = 0.0
    else:
        category = "predicate_violation"
        overall = scenario_score

    return {
        "overall": overall,
        "category": category,
        "compile_score": 1.0 if compiles else 0.0,
        "start_score": 1.0 if starts else 0.0,
        "ingest_score": 1.0 if events_admitted else 0.0,
        "rule_score": 1.0 if rules_registered else 0.0,
        "sink_score": 1.0 if sink_reached else 0.0,
        "scenario_score": scenario_score,
        "assertions_passed": passed,
        "assertions_total": total,
    }
