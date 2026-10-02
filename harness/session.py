"""Drive one riemannd candidate and grade what the sink receiver observed.

The shared Session owns port, working directory, launch, readiness, deadline,
settle, teardown and the report files. This subclass adds what is
riemann-go's: the binding check over a multi-artifact manifest, the sink
receiver as oracle, rule seeding, a stimulus timeline with offsets from the
first stimulus, the rule-counter read that classifies a failure, the trace
built from receiver records plus harness replies, and the score categories.
Nothing here understands a combinator, a threshold or a state.
"""
import json
import time
from typing import Any, Dict, List

from riemann_harness.score import compute
from riemann_harness.session import DEFAULT_READY_TIMEOUT_SECONDS, Session
from riemann_harness.sinks import SinkReceiver

from harness import observer, plan
from harness.adapters.riemannd import Adapter
from harness.binding import verify

# Whole-trial deadline, in seconds, when a scenario names none. Default: the
# longest scenario in the corpus runs about 14 s of stimulus and settle; 120
# leaves room for a slow candidate without letting a hung one stall a corpus.
TRIAL_TIMEOUT_SECONDS = 120
# Settle after the last stimulus, in seconds, when a scenario names none. The
# report records the effective value beside the wall clock.
SETTLE_SECONDS = 5

# op -> (allowed keys, required keys), both excluding "op". Every stimulus
# step carries `at`, its offset from the first stimulus.
STEPS = {
    "emit": ({"at", "events"}, {"at", "events"}),
    "emit_n": ({"at", "count", "batch_size", "interval", "template"}, {"at", "count", "batch_size", "interval", "template"}),
    "put_rule": ({"at", "id", "rule"}, {"at", "id", "rule"}),
    "delete_rule": ({"at", "id"}, {"at", "id"}),
    "dryrun_rule": ({"at", "id", "rule"}, {"at", "id", "rule"}),
    "query_index": ({"at", "q"}, {"at", "q"}),
    "sink_delay": ({"at", "sink", "seconds"}, {"at", "sink", "seconds"}),
    "sink_fail": ({"at", "sink", "status"}, {"at", "sink", "status"}),
}


def expand(template: Dict[str, Any], i: int) -> Dict[str, Any]:
    """Substitute {i} into string values of an event template."""
    return {k: (v.format(i=i) if isinstance(v, str) and "{i}" in v else v)
            for k, v in template.items()}


def _json_object(body) -> Dict[str, Any]:
    return body if isinstance(body, dict) else {}


class RiemannSession(Session):
    steps = STEPS
    profiles = ("sinks",)
    scenario_keys = frozenset({"rules", "ntfy_topic"})
    ready_path = "/healthz"
    work_prefix = "riemann-arena-"
    evidence = "sink-receiver observations over loopback HTTP plus harness-emitted replies"
    # Set per trial by the harness_session_class fixture from the command line.
    spec_path = None
    manifest_path = None
    unbound = False

    def __init__(self, scenario, command, out_dir, ready_timeout=DEFAULT_READY_TIMEOUT_SECONDS):
        super().__init__(scenario, command, out_dir, ready_timeout)
        self.deadline = self.begin + scenario.get("timeout", TRIAL_TIMEOUT_SECONDS)
        self.events: List[Dict[str, Any]] = []
        self.records: List[Dict[str, Any]] = []
        self.receiver = None
        self.binding = None
        self.t0 = None
        self.wall_start = time.time()

    @classmethod
    def validate(cls, scenario):
        plan.validate(scenario)
        return super().validate(scenario)

    # ---- hooks ---------------------------------------------------------------

    def before_launch(self):
        if self.unbound:
            self.binding = {"status": "unbound",
                            "reason": "trial run with --unbound; artifact and SPEC.md bytes were not verified"}
        else:
            self.binding = verify(self.command, self.spec_path, self.manifest_path)[1]
        self.receiver = SinkReceiver().start()

    def make_adapter(self, listen, directory):
        return Adapter(self.command, listen, directory,
                       ntfy_url=self.receiver.ntfy_url,
                       ntfy_topic=self.scenario.get("ntfy_topic", "arena"),
                       influx_url=self.receiver.influx_url,
                       config=self.scenario.get("config") or {})

    def after_ready(self):
        for rule in self.scenario.get("rules") or []:
            self._put_rule(rule["id"], rule)
        self.t0 = time.time()

    def extra_step(self, step):
        if step.get("op") not in STEPS:
            super().extra_step(step)
        sleep_for = self.t0 + float(step["at"]) - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)
        self._apply(step)

    def close_observers(self):
        # After settle and before riemannd stops: failure classification only.
        if self.started and not self.category:
            self._read_rule_counters()

    def after_stop(self):
        if self.receiver:
            self.records = self.receiver.records()
            self.receiver.stop()
        # Rows the shared session recorded (run_error) keep their timestamps and
        # merge with the receiver's records and the harness replies.
        recorded = [dict(row, seq=None) for row in self.trace.rows]
        try:
            rows = observer.build_trace(self.records, self.events + recorded)
        except Exception as exc:  # a malformed body must not look like a clean fail
            rows = []
            self.category, self.error = "observer_error", f"trace build failed: {exc}"
        with self.trace.lock:
            self.trace.rows = rows

    # ---- stimulus ------------------------------------------------------------

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

    def _read_rule_counters(self) -> None:
        """Ask each seeded rule whether any node passed an event. Nothing asserts on it."""
        for rule in self.scenario.get("rules") or []:
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

    # ---- grade ---------------------------------------------------------------

    def report(self):
        report = super().report()
        rows = self.trace.rows
        checks = {f"{op}[{i}]": {"ok": item["ok"]}
                  for op, items in report["assertions"].items() for i, item in enumerate(items)}
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
        # that exited, a scenario with no final assertion) stays the category.
        category = self.category if self.category not in (None, "predicate_violation") else score["category"]
        if category == "GREEN" and not checks:
            category = "invalid_scenario"
        score["category"] = category
        score["overall"] = 1.0 if category == "GREEN" else 0.0
        report.update({
            "category": category,
            "ok": category == "GREEN",
            "score": score,
            "settle_seconds": float(self.scenario.get("settle", SETTLE_SECONDS)),
            "wall_clock_seconds": round(time.time() - self.wall_start, 3),
            "sink_request_count": len(self.records),
            "sink_receiver": self.receiver.base_url if self.receiver else None,
            "listen_addr": self.adapter.listen if self.adapter else None,
            "build_binding": self.binding,
        })
        (self.out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        return report
