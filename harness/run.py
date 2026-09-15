"""Run one scenario against a riemann-go binary.

The harness owns the whole lifecycle:

  1. Start the sink receiver on 127.0.0.1, ephemeral port.
  2. Start riemannd through the adapter, pointed at that receiver.
  3. Wait for ready.
  4. Seed the scenario's rules.
  5. Drive the stimulus timeline.
  6. Settle.
  7. Stop riemannd.
  8. Build the trace from the receiver's records plus harness-emitted events.
  9. Run the matcher. Write reports/<scenario>.json and traces/<run>.jsonl.

The harness understands no combinator, no threshold, no state. It moves bytes
and records what arrived.

Usage:
    python -m harness.run --scenario scenarios/ingest-accepted.yaml \\
        --binary bin/riemannd --report-out reports/ingest-accepted.json

Self-test, needing no binary and no network:
    python -m harness.run --dry
"""

import argparse
import json
import os
import re
import socket
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional

import requests
import yaml

_here = os.path.dirname(os.path.abspath(__file__))
_repo = os.path.dirname(_here)
if _repo not in sys.path:
    sys.path.insert(0, _repo)

from harness import adapters, matcher, observer, score, sinks

_DURATION_RE = re.compile(r"^(?P<n>\d+(?:\.\d+)?)\s*(?P<u>ms|s|m)?$")


def parse_duration(s) -> float:
    if isinstance(s, (int, float)):
        return float(s)
    m = _DURATION_RE.match(str(s).strip())
    if not m:
        raise ValueError(f"bad duration: {s!r}")
    return float(m.group("n")) * {"ms": 0.001, "s": 1.0, "m": 60.0}[m.group("u") or "s"]


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_until_ready(adapter, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if adapter.ready():
            return True
        time.sleep(0.2)
    return False


def _expand(template: Dict[str, Any], i: int) -> Dict[str, Any]:
    """Substitute {i} into string values of an event template."""
    out: Dict[str, Any] = {}
    for k, v in template.items():
        out[k] = v.format(i=i) if isinstance(v, str) and "{i}" in v else v
    return out


class Driver:
    """Applies stimulus events. Owns the harness-emitted trace events."""

    def __init__(self, adapter, receiver: sinks.SinkReceiver):
        self.adapter = adapter
        self.receiver = receiver
        self.events: List[Dict[str, Any]] = []

    def _record_ingest(self, resp: requests.Response) -> None:
        try:
            body = resp.json()
        except Exception:
            body = {}
        self.events.append(
            observer.ingest_response_event(
                time.time(),
                resp.status_code,
                body.get("accepted"),
                body.get("rejected"),
            )
        )

    def apply(self, event: Dict[str, Any]) -> None:
        if "emit" in event:
            spec = event["emit"]
            batch = spec["events"] if isinstance(spec, dict) else spec
            self._record_ingest(self.adapter.post_events(batch))
        elif "emit_n" in event:
            spec = event["emit_n"]
            count = int(spec["count"])
            interval = parse_duration(spec.get("interval", 0))
            batch_size = int(spec.get("batch_size", 1))
            template = spec["template"]
            pending: List[Dict[str, Any]] = []
            for i in range(count):
                pending.append(_expand(template, i))
                if len(pending) >= batch_size:
                    self._record_ingest(self.adapter.post_events(pending))
                    pending = []
                    if interval > 0:
                        time.sleep(interval)
            if pending:
                self._record_ingest(self.adapter.post_events(pending))
        elif "put_rule" in event:
            spec = event["put_rule"]
            rule_id = spec["id"]
            resp = self.adapter.put_rule(rule_id, spec["rule"])
            try:
                version = resp.json().get("version")
            except Exception:
                version = None
            self.events.append(
                observer.rule_response_event(
                    time.time(), "put", rule_id, resp.status_code, version
                )
            )
        elif "delete_rule" in event:
            rule_id = event["delete_rule"]
            resp = self.adapter.delete_rule(rule_id)
            self.events.append(
                observer.rule_response_event(
                    time.time(), "delete", rule_id, resp.status_code, None
                )
            )
        elif "query_index" in event:
            expr = event["query_index"]
            resp = self.adapter.query_index(expr)
            try:
                body = resp.json()
            except Exception:
                body = {}
            results = body.get("events", body if isinstance(body, list) else [])
            self.events.append(
                observer.query_response_event(
                    time.time(),
                    "index",
                    q=expr,
                    status=resp.status_code,
                    match_count=len(results) if isinstance(results, list) else None,
                )
            )
        elif "sink_delay" in event:
            spec = event["sink_delay"]
            self.receiver.set_delay(spec["sink"], parse_duration(spec["seconds"]))
        elif "sink_fail" in event:
            spec = event["sink_fail"]
            self.receiver.set_fail(spec["sink"], spec.get("status"))
        else:
            raise ValueError(f"unknown stimulus event shape: {event}")


def run(args) -> Dict[str, Any]:
    with open(args.scenario) as f:
        scenario = yaml.safe_load(f)

    settle_seconds = float(scenario.get("settle_seconds", args.settle))
    compiles = os.access(args.binary, os.X_OK)

    receiver = sinks.SinkReceiver().start()
    state_dir = tempfile.mkdtemp(prefix="riemann-arena-")
    os.makedirs(os.path.join(state_dir, "rules"), exist_ok=True)
    log_path = os.path.join(state_dir, "riemannd.log")
    listen_addr = args.listen or f"127.0.0.1:{free_port()}"

    adapter = adapters.load(args.adapter)(
        binary=os.path.abspath(args.binary),
        listen_addr=listen_addr,
        ntfy_url=receiver.ntfy_url,
        ntfy_topic=scenario.get("ntfy_topic", "arena"),
        influx_url=receiver.influx_url,
        log_path=log_path,
        state_dir=state_dir,
        config=scenario.get("config") or {},
    )

    wall_start = time.time()
    started = False
    try:
        if not compiles:
            return _finalize(
                args, scenario, receiver, [],
                compiles=False, starts=False, observer_ok=True,
                warnings=[f"binary not executable: {args.binary}"],
                settle_seconds=settle_seconds, wall_start=wall_start,
                log_path=None, listen_addr=listen_addr,
            )

        adapter.start()
        started = wait_until_ready(adapter, timeout=args.ready_timeout)
        if not started:
            return _finalize(
                args, scenario, receiver, [],
                compiles=True, starts=False, observer_ok=True,
                warnings=["riemannd did not become ready before the timeout"],
                settle_seconds=settle_seconds, wall_start=wall_start,
                log_path=log_path, listen_addr=listen_addr,
            )

        driver = Driver(adapter, receiver)
        for rule in scenario.get("rules", []) or []:
            driver.apply({"put_rule": {"id": rule["id"], "rule": rule}})

        t0 = time.time()
        for event in scenario.get("stimulus", []) or []:
            sleep_for = t0 + parse_duration(event["at"]) - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)
            driver.apply(event)

        time.sleep(settle_seconds)
        return _finalize(
            args, scenario, receiver, driver.events,
            compiles=True, starts=True, observer_ok=True, warnings=[],
            settle_seconds=settle_seconds, wall_start=wall_start,
            log_path=log_path, listen_addr=listen_addr,
        )
    finally:
        adapter.stop()
        receiver.stop()


def _finalize(
    args, scenario, receiver, harness_events, *, compiles, starts, observer_ok,
    warnings, settle_seconds, wall_start, log_path, listen_addr,
) -> Dict[str, Any]:
    records = receiver.records()
    try:
        trace = observer.build_trace(records, harness_events)
    except Exception as e:  # a malformed body must not look like a clean fail
        trace, observer_ok = [], False
        warnings = list(warnings) + [f"trace build failed: {e}"]

    ok, matcher_report = matcher.evaluate(scenario.get("expect", {}), trace)
    sink_connected = any(e.get("source") == "sink-receiver" for e in trace)

    s = score.compute(
        compiles=compiles,
        starts=starts,
        sink_connected=sink_connected,
        observer_ok=observer_ok,
        predicate_results=_assertion_summary(matcher_report),
    )
    report = {
        "scenario": args.scenario,
        "scenario_name": scenario.get("name"),
        "scenario_description": scenario.get("description"),
        "binary": args.binary,
        "adapter": args.adapter,
        "listen_addr": listen_addr,
        "sink_receiver": receiver.base_url,
        "settle_seconds": settle_seconds,
        "wall_clock_seconds": round(time.time() - wall_start, 3),
        "sink_request_count": len(records),
        "warnings": warnings,
        "score": s,
        "matcher": matcher_report,
        "trace": trace,
        "riemannd_log_tail": _tail(log_path),
    }
    _write_outputs(args, report, trace)
    return report


def _write_outputs(args, report: Dict[str, Any], trace: List[Dict[str, Any]]) -> None:
    if args.report_out:
        os.makedirs(os.path.dirname(os.path.abspath(args.report_out)) or ".", exist_ok=True)
        with open(args.report_out, "w") as f:
            json.dump(report, f, indent=2)
    if args.trace_out:
        os.makedirs(os.path.dirname(os.path.abspath(args.trace_out)) or ".", exist_ok=True)
        with open(args.trace_out, "w") as f:
            for ev in trace:
                f.write(json.dumps(ev) + "\n")


def _assertion_summary(matcher_report: dict) -> dict:
    out = {}
    for section, items in matcher_report.items():
        for i, item in enumerate(items):
            out[f"{section}[{i}]"] = {"ok": item.get("ok", False)}
    return out


def _tail(path: Optional[str], n: int = 40) -> str:
    if not path:
        return ""
    try:
        with open(path) as f:
            return "".join(f.readlines()[-n:])
    except Exception:
        return ""


# ---- self-test -----------------------------------------------------------


def dry_run(fixture_path: str, scenario_path: str) -> int:
    """Prove the receiver, the trace builder and the matcher work end to end,
    with no riemann-go binary anywhere.

    The fixture is a list of recorded HTTP requests. They are replayed at a
    live sink receiver over real HTTP, so the receiver's parsing is exercised
    rather than bypassed, and the trace is built from what it captured.
    """
    with open(fixture_path) as f:
        fixture = json.load(f)
    with open(scenario_path) as f:
        scenario = yaml.safe_load(f)

    receiver = sinks.SinkReceiver().start()
    try:
        print(f"sink receiver listening on {receiver.base_url}")
        for req in fixture["requests"]:
            url = receiver.base_url + req["path"]
            resp = requests.post(
                url,
                data=req["body"].encode("utf-8"),
                headers=req.get("headers", {}),
                timeout=5,
            )
            print(f"  replayed POST {req['path']} -> {resp.status_code}")

        harness_events = fixture.get("harness_events", [])
        # Fixture timestamps are relative; anchor them so ordering is stable
        # and the trace reads like a real run.
        base = time.time()
        for ev in harness_events:
            ev["ts"] = base + float(ev.get("ts", 0.0))
        records = receiver.records()
        for i, rec in enumerate(records):
            rec["ts"] = base + 0.001 * (i + 1)

        trace = observer.build_trace(records, harness_events)
        print(f"\ntrace: {len(trace)} events from {len(records)} sink requests")
        for ev in trace:
            print(f"  seq={ev['seq']} {ev['source']:14s} {ev['event']}")

        ok, report = matcher.evaluate(scenario.get("expect", {}), trace)
        print(f"\nmatcher assertions:")
        for section, items in report.items():
            for item in items:
                print(f"  {'PASS' if item['ok'] else 'FAIL'}  {section}[{item['index']}]")

        # The self-test also proves a violated property fails, not just that a
        # satisfied one passes. A matcher that always passes is worthless.
        negative = {"trace": {"contains": [{"event": "ntfy_post", "state": "no-such-state"}]}}
        neg_ok, _ = matcher.evaluate(negative, trace)

        print(f"\npositive scenario ok: {ok}")
        print(f"negative control ok:  {neg_ok} (must be False)")
        if ok and not neg_ok:
            print("\nSELF-TEST GREEN")
            return 0
        print("\nSELF-TEST FAILED")
        return 1
    finally:
        receiver.stop()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scenario")
    p.add_argument("--binary")
    p.add_argument("--adapter", default="riemannd")
    p.add_argument("--listen", default=None, help="riemannd listen addr; default ephemeral")
    p.add_argument("--settle", type=float, default=5.0)
    p.add_argument("--ready-timeout", type=float, default=15.0)
    p.add_argument("--report-out", default=None)
    p.add_argument("--trace-out", default=None)
    p.add_argument("--dry", action="store_true", help="self-test; needs no binary")
    p.add_argument("--fixture", default=os.path.join(_repo, "scenarios/fixtures/dry-run.json"))
    p.add_argument(
        "--fixture-scenario",
        default=os.path.join(_repo, "scenarios/fixtures/dry-run.yaml"),
    )
    args = p.parse_args()

    if args.dry:
        sys.exit(dry_run(args.fixture, args.fixture_scenario))

    if not args.scenario or not args.binary:
        p.error("--scenario and --binary are required unless --dry is given")

    report = run(args)
    s = report["score"]
    print(f"\n{'=' * 60}")
    print(f"scenario:   {report.get('scenario_name')}")
    print(f"category:   {s['category']}")
    print(f"overall:    {s['overall']:.2f}  ({s['assertions_passed']}/{s['assertions_total']} assertions)")
    print(f"settle:     {report['settle_seconds']}s   wall clock: {report['wall_clock_seconds']}s")
    print(f"sink posts: {report['sink_request_count']}")
    for w in report["warnings"]:
        print(f"warning:    {w}")
    print(f"{'=' * 60}\n")
    sys.exit(0 if s["overall"] == 1.0 else 1)


if __name__ == "__main__":
    main()
