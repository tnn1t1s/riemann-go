"""Drive one riemannd candidate and grade what the sink receiver observed.

The session owns the whole lifecycle: start the sink receiver on an ephemeral
loopback port, start riemannd through the adapter pointed at it, wait for
ready, seed the rules, drive the stimulus timeline, settle, read the rule
counters, stop riemannd, stop the receiver, build the trace, evaluate. The
Gherkin steps call into this object; nothing here understands a combinator, a
threshold or a state.
"""
import hashlib
import json
import socket
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from riemann_harness.matcher import evaluate
from riemann_harness.score import compute
from riemann_harness.sinks import SinkReceiver

from harness import observer
from harness.adapters.riemannd import Adapter


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def expand(template: Dict[str, Any], i: int) -> Dict[str, Any]:
    """Substitute {i} into string values of an event template."""
    return {k: (v.format(i=i) if isinstance(v, str) and "{i}" in v else v)
            for k, v in template.items()}


def _json_object(body) -> Dict[str, Any]:
    return body if isinstance(body, dict) else {}


class Session:
    """One isolated candidate. Steps drive it immediately; close() grades."""

    def __init__(self, plan: Dict[str, Any], command: List[str], out_dir, ready_timeout: float = 15.0):
        self.plan, self.command = plan, list(command)
        self.out = Path(out_dir)
        self.out.mkdir(parents=True, exist_ok=True)
        self.ready_timeout = ready_timeout
        self.events: List[Dict[str, Any]] = []
        self.rows: List[Dict[str, Any]] = []
        self.started = False
        self.closed = False
        self.category: Optional[str] = None
        self.error: Optional[str] = None
        self.warnings: List[str] = []
        self.receiver: Optional[SinkReceiver] = None
        self.adapter: Optional[Adapter] = None
        self.state_dir: Optional[tempfile.TemporaryDirectory] = None
        self.log = ""
        self.listen_addr = None
        self.t0: Optional[float] = None
        self.wall_start = time.time()
        self.records: List[Dict[str, Any]] = []

    # ---- lifecycle ---------------------------------------------------------

    def fail(self, exc, category: str = "driver_error") -> None:
        self.category = self.category or category
        self.error = str(exc)

    def start(self) -> None:
        try:
            self.receiver = SinkReceiver().start()
            self.state_dir = tempfile.TemporaryDirectory(prefix="riemann-arena-")
            self.listen_addr = f"127.0.0.1:{free_port()}"
            self.adapter = Adapter(
                command=self.command,
                listen_addr=self.listen_addr,
                ntfy_url=self.receiver.ntfy_url,
                ntfy_topic=self.plan.get("ntfy_topic", "arena"),
                influx_url=self.receiver.influx_url,
                log_path=str(Path(self.state_dir.name) / "riemannd.log"),
                state_dir=self.state_dir.name,
                config=self.plan.get("config") or {},
            )
            self.adapter.start()
            deadline = time.monotonic() + self.ready_timeout
            while time.monotonic() < deadline:
                if self.adapter.ready():
                    self.started = True
                    break
                if self.adapter.exited() is not None:
                    break
                time.sleep(0.2)
            if not self.started:
                self.category = "start_error"
                raise RuntimeError("riemannd did not become ready before the timeout")
            for rule in self.plan.get("rules") or []:
                self._put_rule(rule["id"], rule)
            self.t0 = time.time()
        except Exception as exc:
            self.fail(exc)
            raise

    def step(self, step: Dict[str, Any]) -> None:
        if self.closed:
            raise ValueError("actions cannot follow final trace assertions")
        try:
            sleep_for = self.t0 + float(step["at"]) - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._apply(step)
        except Exception as exc:
            self.fail(exc)
            raise

    # ---- stimulus ----------------------------------------------------------

    def _record_ingest(self, response) -> None:
        status, _, body = response
        body = _json_object(body)
        self.events.append(observer.ingest_response_event(
            time.time(), status, body.get("accepted"), body.get("rejected")))

    def _put_rule(self, rule_id: str, rule: Dict[str, Any]) -> None:
        status, _, body = self.adapter.put_rule(rule_id, rule)
        self.events.append(observer.rule_response_event(
            time.time(), "put", rule_id, status, _json_object(body).get("version")))

    def _apply(self, step: Dict[str, Any]) -> None:
        op = step["op"]
        if op == "emit":
            self._record_ingest(self.adapter.post_events(step["events"]))
        elif op == "emit_n":
            pending: List[Dict[str, Any]] = []
            for i in range(int(step["count"])):
                pending.append(expand(step["template"], i))
                if len(pending) >= int(step["batch_size"]):
                    self._record_ingest(self.adapter.post_events(pending))
                    pending = []
                    if step["interval"] > 0:
                        time.sleep(step["interval"])
            if pending:
                self._record_ingest(self.adapter.post_events(pending))
        elif op == "put_rule":
            self._put_rule(step["id"], step["rule"])
        elif op == "dryrun_rule":
            status, _, body = self.adapter.dryrun_rule(step["id"], step["rule"])
            firings = _json_object(body).get("firings", [])
            ev = observer.rule_response_event(time.time(), "dryrun", step["id"], status, None)
            ev["firing_count"] = len(firings) if isinstance(firings, list) else None
            self.events.append(ev)
        elif op == "delete_rule":
            status, _, _ = self.adapter.delete_rule(step["id"])
            self.events.append(observer.rule_response_event(time.time(), "delete", step["id"], status, None))
        elif op == "query_index":
            status, headers, body = self.adapter.query_index(step["q"])
            # GET /index answers {"as_of":..., "entries":[...]}; GET /events
            # answers {"events":[...]}. Both shapes are read so a query records
            # the count it actually got rather than zero by accident.
            if isinstance(body, list):
                results = body
            else:
                body = _json_object(body)
                results = body.get("entries", body.get("events", []))
            lowered = {k.lower(): v for k, v in headers.items()}
            self.events.append(observer.query_response_event(
                time.time(), "index", q=step["q"], status=status,
                match_count=len(results) if isinstance(results, list) else None,
                # Mechanical: the header is present or it is not.
                allow_origin=lowered.get("access-control-allow-origin")))
        elif op == "sink_delay":
            self.receiver.set_delay(step["sink"], float(step["seconds"]))
        elif op == "sink_fail":
            self.receiver.set_fail(step["sink"], step["status"])
        else:
            raise ValueError("unknown stimulus op: " + repr(op))

    # ---- settle and grade ----------------------------------------------------

    def _read_rule_counters(self) -> None:
        """After settle, ask each seeded rule whether any node passed an event.

        Failure classification only. Nothing asserts on it.
        """
        for rule in self.plan.get("rules") or []:
            try:
                status, _, body = self.adapter.get_rule(rule["id"])
            except Exception:
                continue
            fired = False
            if 200 <= status < 300:
                counters = _json_object(body).get("counters") or {}
                fired = any((counters.get(k) or 0) > 0 for k in counters)
            self.events.append(observer.rule_response_event(
                time.time(), "get", rule["id"], status, None, fired=fired))

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            if self.started and not self.category:
                time.sleep(float(self.plan.get("settle", 5)))
                if self.adapter.exited() is not None:
                    self.category = "process_error"
                    raise RuntimeError("riemannd exited during the trial")
                self._read_rule_counters()
        except Exception as exc:
            self.fail(exc)
        finally:
            try:
                if self.adapter:
                    self.adapter.stop()
                    log_path = Path(self.adapter.log_path)
                    if log_path.exists():
                        self.log = log_path.read_text(errors="replace")
            finally:
                try:
                    if self.receiver:
                        self.records = self.receiver.records()
                        self.receiver.stop()
                finally:
                    if self.state_dir:
                        self.state_dir.cleanup()
        try:
            self.rows = observer.build_trace(self.records, self.events)
        except Exception as exc:  # a malformed body must not look like a clean fail
            self.rows = []
            self.category, self.error = "observer_error", f"trace build failed: {exc}"

    def trace(self) -> List[Dict[str, Any]]:
        return json.loads(json.dumps(self.rows))

    def report(self) -> Dict[str, Any]:
        self.close()
        rows = self.rows
        ok, assertions = evaluate(self.plan.get("expect", {"trace": {}}), rows)
        checks = {f"{op}[{i}]": {"ok": item["ok"]}
                  for op, items in assertions.items() for i, item in enumerate(items)}
        events_admitted = any(e.get("event") == "ingest_response" and (e.get("accepted") or 0) > 0 for e in rows)
        puts = [e for e in rows if e.get("event") == "rule_response" and e.get("kind") == "put"]
        rules_registered = (not puts) or any(200 <= (e.get("status") or 0) < 300 for e in puts)
        gets = [e for e in rows if e.get("event") == "rule_response" and e.get("kind") == "get"]
        rules_fired = (not gets) or any(e.get("fired") for e in gets)
        score = compute(
            compiles=True,
            starts=self.started,
            events_admitted=events_admitted,
            rules_registered=rules_registered and rules_fired,
            sink_reached=any(e.get("source") == "sink-receiver" for e in rows),
            observer_ok=self.category != "observer_error",
            predicate_results=checks,
        )
        # A session failure the score cannot see (a step that raised, a process
        # that exited, a scenario with no final assertion) is the category.
        category = score["category"]
        if self.category and self.category not in ("predicate_violation",):
            category = self.category
        if category == "GREEN" and not checks:
            category = "invalid_scenario"
        score["category"] = category
        score["overall"] = 1.0 if category == "GREEN" else 0.0
        report = {
            "scenario": self.plan.get("name"),
            "category": category,
            "ok": category == "GREEN",
            "error": self.error,
            "score": score,
            "assertions": assertions,
            "assertions_passed": score["assertions_passed"],
            "assertions_total": score["assertions_total"],
            "settle_seconds": float(self.plan.get("settle", 5)),
            "wall_clock_seconds": round(time.time() - self.wall_start, 3),
            "sink_request_count": len(self.records),
            "sink_receiver": self.receiver.base_url if self.receiver else None,
            "listen_addr": self.listen_addr,
            "command": self.command,
            "plan_sha256": hashlib.sha256(json.dumps(self.plan, sort_keys=True).encode()).hexdigest(),
            "warnings": self.warnings,
            "evidence": "sink-receiver observations over loopback HTTP plus harness-emitted replies",
        }
        (self.out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        (self.out / "trace.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
        # The diagnostician reads the whole log; written even when empty so an
        # absent file means the run did not get that far.
        (self.out / "candidate.log").write_text(self.log)
        return report
