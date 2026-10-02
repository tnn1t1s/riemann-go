"""Replay recorded sink requests through a live receiver and grade the trace.

The fixture is a list of recorded HTTP requests. They are replayed at a live
sink receiver over real HTTP, so the receiver's parsing is exercised rather
than bypassed, and the trace is built from what it captured. The negative
control proves a violated property fails: a matcher that always passes would
pass every scenario ever written.
"""
import json
import time
import urllib.request
from pathlib import Path

from riemann_harness.matcher import evaluate
from riemann_harness.sinks import SinkReceiver

from harness import observer

FIXTURES = Path(__file__).with_name("fixtures")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def replayed_trace():
    fixture = json.loads((FIXTURES / "dry-run.json").read_text())
    with SinkReceiver() as receiver:
        for req in fixture["requests"]:
            r = urllib.request.Request(receiver.base_url + req["path"], data=req["body"].encode(),
                                       headers=req.get("headers", {}), method="POST")
            with OPENER.open(r, timeout=5):
                pass
        base = time.time()
        harness_events = fixture.get("harness_events", [])
        for ev in harness_events:
            ev["ts"] = base + float(ev.get("ts", 0.0))
        records = receiver.records()
    for i, rec in enumerate(records):
        rec["ts"] = base + 0.001 * (i + 1)
    return observer.build_trace(records, harness_events)


def test_fixture_replay_satisfies_every_operator():
    trace = replayed_trace()
    expect = json.loads((FIXTURES / "dry-run-expect.json").read_text())
    ok, report = evaluate(expect, trace)
    assert ok, json.dumps(report, indent=2)
    assert all(report[op] for op in ("contains", "not_contains", "order", "count", "field_exists"))
    assert [row["seq"] for row in trace] == list(range(1, len(trace) + 1))


def test_negative_control_fails():
    trace = replayed_trace()
    ok, _ = evaluate({"trace": {"contains": [{"event": "ntfy_post", "state": "no-such-state"}]}}, trace)
    assert not ok


def test_provenance_absent_reads_as_null():
    assert observer.ntfy_provenance({"message": "ghost x is ok"}) == {k: None for k in observer._PROVENANCE_KEYS}
    assert observer.ntfy_provenance({"message": "line\nriemann-go: not json"})["rule"] is None
    found = observer.ntfy_provenance({"message": "line\nriemann-go: {\"rule\": \"r\", \"node\": \"stream\"}"})
    assert found["rule"] == "r" and found["node"] == "stream" and found["prior_state"] is None


def test_malformed_influx_body_is_not_a_clean_trace():
    rows = observer.build_trace([{"ts": 1.0, "sink": "influx", "query": {}, "body": "nonsense\n"}], [])
    assert rows == []  # a line with no field set is not a point; nothing is invented for it
