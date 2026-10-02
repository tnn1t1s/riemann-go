"""pytest-bdd bindings. Gherkin owns syntax; the sink receiver owns evidence.

Each step appends to a plan the session executes immediately. Given steps
configure; When steps are the stimulus timeline, each carrying its offset from
the first stimulus; the first Then step settles, stops the candidate, builds
the trace and grades. Later Then steps grade the same frozen trace. Nothing
here names a combinator, a threshold or a state.
"""
import hashlib
import json
import os
import re
from pathlib import Path

import pytest
from pytest_bdd import given, parsers, then, when

from riemann_harness.matcher import evaluate

from harness.binding import verify
from harness.plan import validate, validate_expect, validate_step
from harness.session import Session

ROOT = Path(__file__).resolve().parents[1]


def pytest_addoption(parser):
    group = parser.getgroup("riemann-go")
    group.addoption("--riemann-command", help="Candidate command as a JSON argv array")
    group.addoption("--riemann-out", default="reports/trial")
    group.addoption("--riemann-validate", action="store_true",
                    help="Check feature bindings and data without running a candidate")
    group.addoption("--riemann-ready-timeout", type=float, default=15.0,
                    help="Seconds to wait for GET /healthz to answer 200 after launch")
    group.addoption("--riemann-spec", default=str(ROOT / "SPEC.md"),
                    help="The SPEC.md the candidate was generated from")
    group.addoption("--riemann-build-manifest",
                    help="build-manifest.json binding the artifact to that SPEC.md")
    group.addoption("--riemann-unbound", action="store_true",
                    help="Launch without a build manifest; the report records the waiver")


def pytest_configure(config):
    config._riemann_reports = []


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    setattr(item, "riemann_" + outcome.get_result().when, outcome.get_result())


def pytest_bdd_before_scenario(request, feature, scenario):
    request.node.riemann_feature = {
        "feature_file": str(feature.filename),
        "feature_sha256": hashlib.sha256(Path(feature.filename).read_bytes()).hexdigest(),
        "scenario_name": scenario.name,
        "tags": sorted(scenario.tags),
    }


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    config = session.config
    if not config.getoption("--riemann-command"):
        return
    reporter = config.pluginmanager.getplugin("terminalreporter")
    # A skipped, xfailed or xpassed case cannot turn a candidate trial green.
    if reporter and any(reporter.stats.get(k) for k in ("skipped", "xfailed", "xpassed")):
        session.exitstatus = 1
    reports = config._riemann_reports
    if not reports:
        return
    out = Path(config.getoption("--riemann-out")).resolve()
    out.mkdir(parents=True, exist_ok=True)
    green = sum(1 for r in reports if r["category"] == "GREEN")
    summary = {
        "command": json.loads(config.getoption("--riemann-command")),
        "total": len(reports), "green": green,
        "categories": sorted({r["category"] for r in reports}),
        "scenarios": reports,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    if reporter:
        reporter.write_line("")
        reporter.write_line("riemann-go trial summary")
        for r in reports:
            reporter.write_line(f"  {r['scenario']:<48} {r['category']}")
        reporter.write_line(f"  -- {green}/{len(reports)} green; summary at {out / 'summary.json'}")


class Arena:
    def __init__(self, request):
        self.request = request
        config = request.config
        self.validation = config.getoption("--riemann-validate")
        feature = getattr(request.node, "riemann_feature", {})
        self.plan = {"name": feature.get("scenario_name") or request.node.name,
                     "rules": [], "steps": [], "expect": {"trace": {}}}
        self.session = None
        self.binding = None
        self.finalized = False
        key = hashlib.sha256(request.node.nodeid.encode()).hexdigest()[:8]
        slug = re.sub(r"[^A-Za-z0-9_-]+", "_", self.plan["name"])[:100]
        self.out = Path(config.getoption("--riemann-out")).resolve() / (slug + "-" + key)

    def configure(self, key, value):
        if self.plan["steps"] or self.finalized:
            raise ValueError("configuration must precede the stimulus timeline")
        self.plan[key] = value

    def action(self, step):
        if self.finalized:
            raise ValueError("actions cannot follow final trace assertions")
        validate_step(step)
        self.plan["steps"].append(step)
        if self.validation:
            return
        if self.session is None:
            self._launch()
        self.session.step(step)

    def _launch(self):
        config = self.request.config
        raw = config.getoption("--riemann-command")
        if not raw:
            raise ValueError("candidate required: pass --riemann-command or use --riemann-validate")
        command = json.loads(raw)
        if not isinstance(command, list) or not command or not all(isinstance(v, str) and v for v in command):
            raise ValueError("candidate command must be a nonempty JSON argv array")
        if config.getoption("--riemann-unbound"):
            if not Path(command[0]).is_absolute() or not os.access(command[0], os.X_OK):
                raise ValueError("an absolute executable artifact path is required as command[0]")
            self.binding = {"status": "unbound",
                            "reason": "trial run with --unbound; artifact and SPEC.md bytes were not verified"}
        else:
            self.binding = verify(command, config.getoption("--riemann-spec"),
                                  config.getoption("--riemann-build-manifest"))[1]
        self.session = Session(self.plan, command, self.out, config.getoption("--riemann-ready-timeout"))
        self.session.start()

    def assertion(self, op, entries):
        if not isinstance(entries, list) or not entries:
            raise ValueError("a trace assertion needs a nonempty JSON array")
        self.plan["expect"]["trace"].setdefault(op, []).extend(entries)
        validate_expect(self.plan["expect"])
        self.finalized = True
        if self.validation:
            return
        if self.session is None:
            raise ValueError("no candidate actions were executed")
        self.session.close()
        if self.session.category:
            raise AssertionError(f"{self.session.category}: {self.session.error}")
        ok, detail = evaluate({"trace": {op: entries}}, self.session.trace())
        if not ok:
            self.session.category = "predicate_violation"
            raise AssertionError(json.dumps(detail[op], indent=2))


@pytest.fixture
def arena(request):
    arena = Arena(request)
    yield arena
    call = getattr(request.node, "riemann_call", None)
    failed = call is not None and not call.passed
    feature = getattr(request.node, "riemann_feature", {})
    if arena.session:
        if failed:
            arena.session.fail("BDD step failed; see pytest report", "bdd_error")
        if not arena.finalized:
            arena.session.fail("scenario has no final trace assertions", "invalid_scenario")
        report = arena.session.report()
        report.update(feature)
        report["build_binding"] = arena.binding
        (arena.out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        request.config._riemann_reports.append({
            "scenario": report["scenario"], "category": report["category"],
            "assertions_passed": report["assertions_passed"],
            "assertions_total": report["assertions_total"],
            "wall_clock_seconds": report["wall_clock_seconds"],
            "report_dir": str(arena.out)})
        request.node.user_properties.append(("category", report["category"]))
    if not failed:
        validate(arena.plan)
        assert arena.finalized, "scenario has no final trace assertions"
        if arena.validation:
            arena.out.mkdir(parents=True, exist_ok=True)
            # Inspection artifact only, never a second authored scenario or a verdict.
            (arena.out / "validated-plan.json").write_text(json.dumps(arena.plan, indent=2) + "\n")


# ---- configuration -----------------------------------------------------------

@given("riemannd is configured with:")
def configuration(arena, docstring):
    arena.configure("config", json.loads(docstring))


@given(parsers.parse("a settle window of {seconds:g} seconds"))
def settle(arena, seconds):
    arena.configure("settle", seconds)


@given(parsers.parse('the ntfy topic is "{topic}"'))
def topic(arena, topic):
    arena.configure("ntfy_topic", topic)


@given("these rules are installed:")
def rules(arena, docstring):
    arena.configure("rules", json.loads(docstring))


# ---- stimulus timeline; offsets are seconds from the first stimulus ------------

@when(parsers.parse("at {at:g}s the emitter posts:"))
def emit(arena, at, docstring):
    arena.action({"op": "emit", "at": at, "events": json.loads(docstring)})


@when(parsers.parse("at {at:g}s the emitter posts {count:d} events in batches of {batch:d} every {interval:g}s from the template:"))
def emit_n(arena, at, count, batch, interval, docstring):
    arena.action({"op": "emit_n", "at": at, "count": count, "batch_size": batch,
                  "interval": interval, "template": json.loads(docstring)})


@when(parsers.parse('at {at:g}s the client puts rule "{rule_id}":'))
def put_rule(arena, at, rule_id, docstring):
    arena.action({"op": "put_rule", "at": at, "id": rule_id, "rule": json.loads(docstring)})


@when(parsers.parse('at {at:g}s the client deletes rule "{rule_id}"'))
def delete_rule(arena, at, rule_id):
    arena.action({"op": "delete_rule", "at": at, "id": rule_id})


@when(parsers.parse('at {at:g}s the client dry-runs rule "{rule_id}":'))
def dryrun_rule(arena, at, rule_id, docstring):
    arena.action({"op": "dryrun_rule", "at": at, "id": rule_id, "rule": json.loads(docstring)})


@when(parsers.parse('at {at:g}s the client queries the index with "{expr}"'))
def query_index(arena, at, expr):
    arena.action({"op": "query_index", "at": at, "q": expr})


@when(parsers.parse("at {at:g}s the {sink:w} sink delays each reply by {seconds:g} seconds"))
def sink_delay(arena, at, sink, seconds):
    arena.action({"op": "sink_delay", "at": at, "sink": sink, "seconds": seconds})


@when(parsers.parse("at {at:g}s the {sink:w} sink fails every request with {status:d}"))
def sink_fail(arena, at, sink, status):
    arena.action({"op": "sink_fail", "at": at, "sink": sink, "status": status})


@when(parsers.parse("at {at:g}s the {sink:w} sink replies normally again"))
def sink_recover(arena, at, sink):
    arena.action({"op": "sink_fail", "at": at, "sink": sink, "status": None})


# ---- assertions; the first one settles and closes the trial ------------------

@then("the recorded trace contains:")
def contains(arena, docstring):
    arena.assertion("contains", json.loads(docstring))


@then("the recorded trace excludes:")
def excludes(arena, docstring):
    arena.assertion("not_contains", json.loads(docstring))


@then("the recorded trace has these counts:")
def counts(arena, docstring):
    arena.assertion("count", json.loads(docstring))


@then("the recorded trace has this order:")
def order(arena, docstring):
    arena.assertion("order", json.loads(docstring))


@then("the recorded trace has these fields:")
def fields(arena, docstring):
    arena.assertion("field_exists", json.loads(docstring))
