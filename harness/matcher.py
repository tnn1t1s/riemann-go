"""Generic trace-pattern matcher.

Copied from the cue arena (`cue/harness/matcher.py`). Only this docstring
differs, because the original names cue's domain nouns; every line of code
below is byte-identical, and that is deliberate.

The harness does not understand throttling, expiry, state transitions,
backpressure, or any other domain concept. A scenario declares the trace it
expects. The matcher checks whether the observed trace satisfies the
declaration. Five operators. Field-by-field equality. No special cases.

See HARNESS.md for the operator vocabulary.
"""

from typing import List, Dict, Any, Tuple


def event_matches(event: Dict[str, Any], pattern: Dict[str, Any]) -> bool:
    """An event matches iff every key in pattern equals the same key in event.
    Keys absent from pattern are ignored. Exact equality only."""
    for k, v in pattern.items():
        if event.get(k) != v:
            return False
    return True


def _matches(trace: List[Dict[str, Any]], pattern: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [e for e in trace if event_matches(e, pattern)]


def evaluate(expect: Dict[str, Any], trace: List[Dict[str, Any]]) -> Tuple[bool, Dict[str, Any]]:
    """Evaluate a scenario's expect.trace block against the observed trace.

    Returns (ok, report). The report has one section per operator with per-entry
    results, suitable for direct inclusion in the harness's run report.
    """
    report: Dict[str, Any] = {
        "contains": [],
        "not_contains": [],
        "order": [],
        "count": [],
        "field_exists": [],
    }

    trace_block = expect.get("trace", {}) if isinstance(expect, dict) else {}

    # contains: each pattern must match >= 1 event
    for i, pat in enumerate(trace_block.get("contains", []) or []):
        ms = _matches(trace, pat)
        report["contains"].append(
            {
                "index": i,
                "pattern": pat,
                "match_count": len(ms),
                "first_seq": ms[0]["seq"] if ms else None,
                "ok": len(ms) > 0,
            }
        )

    # not_contains: no pattern may match
    for i, pat in enumerate(trace_block.get("not_contains", []) or []):
        ms = _matches(trace, pat)
        report["not_contains"].append(
            {
                "index": i,
                "pattern": pat,
                "match_count": len(ms),
                "ok": len(ms) == 0,
            }
        )

    # order: before.seq < after.seq; both must match at least once
    for i, entry in enumerate(trace_block.get("order", []) or []):
        before_pat = entry.get("before", {})
        after_pat = entry.get("after", {})
        before_ms = _matches(trace, before_pat)
        after_ms = _matches(trace, after_pat)
        ok = False
        reason = None
        if not before_ms:
            reason = "before pattern did not match any event"
        elif not after_ms:
            reason = "after pattern did not match any event"
        else:
            ok = before_ms[0]["seq"] < after_ms[0]["seq"]
            if not ok:
                reason = (
                    f"before.seq={before_ms[0]['seq']} not less than "
                    f"after.seq={after_ms[0]['seq']}"
                )
        report["order"].append(
            {
                "index": i,
                "before": before_pat,
                "after": after_pat,
                "before_first_seq": before_ms[0]["seq"] if before_ms else None,
                "after_first_seq": after_ms[0]["seq"] if after_ms else None,
                "ok": ok,
                "reason": reason,
            }
        )

    # count: equals / min / max bounds
    for i, entry in enumerate(trace_block.get("count", []) or []):
        pat = entry.get("match", {})
        ms = _matches(trace, pat)
        n = len(ms)
        ok = True
        if "exact" in entry:
            ok = ok and n == entry["exact"]
        if "equals" in entry:
            ok = ok and n == entry["equals"]
        if "min" in entry:
            ok = ok and n >= entry["min"]
        if "max" in entry:
            ok = ok and n <= entry["max"]
        report["count"].append(
            {
                "index": i,
                "pattern": pat,
                "match_count": n,
                "equals": entry.get("equals", entry.get("exact")),
                "min": entry.get("min"),
                "max": entry.get("max"),
                "ok": ok,
            }
        )

    # field_exists: pattern matches; then every match must have field present and non-null
    for i, entry in enumerate(trace_block.get("field_exists", []) or []):
        pat = entry.get("match", {})
        field = entry.get("field")
        ms = _matches(trace, pat)
        ok = bool(ms) and all(
            (field in m) and (m.get(field) not in (None, ""))
            for m in ms
        )
        report["field_exists"].append(
            {
                "index": i,
                "pattern": pat,
                "field": field,
                "match_count": len(ms),
                "ok": ok,
            }
        )

    overall_ok = (
        all(x["ok"] for x in report["contains"])
        and all(x["ok"] for x in report["not_contains"])
        and all(x["ok"] for x in report["order"])
        and all(x["ok"] for x in report["count"])
        and all(x["ok"] for x in report["field_exists"])
    )
    return overall_ok, report
