"""riemann-go step table. The arena, the generic assertions and the trial options come from riemann_harness.

Each step appends one plan entry the session executes immediately. Given steps
configure; When steps are the stimulus timeline, each carrying its offset from
the first stimulus; the first Then step settles, stops the candidate, builds
the trace and grades. Nothing here names a combinator, a threshold or a state.
"""
import json
import time
from pathlib import Path

import pytest
from pytest_bdd import given, parsers, when

from harness.session import RiemannSession


def pytest_addoption(parser):
    group = parser.getgroup("riemann-go")
    group.addoption("--riemann-unbound", action="store_true",
                    help="Launch without a build manifest; the report records the waiver")


def pytest_configure(config):
    config._riemann_started = time.time()


@pytest.fixture
def harness_session_class(request):
    config = request.config
    RiemannSession.spec_path = config.getoption("--harness-spec")
    RiemannSession.manifest_path = config.getoption("--harness-build-manifest")
    RiemannSession.unbound = config.getoption("--riemann-unbound")
    return RiemannSession


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    config = session.config
    if not config.getoption("--harness-command"):
        return
    out = Path(config.getoption("--harness-out")).resolve()
    reports = []
    for path in sorted(out.glob("*/report.json")):
        if path.stat().st_mtime < config._riemann_started:
            continue
        r = json.loads(path.read_text())
        reports.append({"scenario": r.get("scenario_name", r["scenario"]), "category": r["category"],
                        "assertions_passed": r["assertions_passed"], "assertions_total": r["assertions_total"],
                        "wall_clock_seconds": r.get("wall_clock_seconds"), "report_dir": str(path.parent)})
    if not reports:
        return
    green = sum(1 for r in reports if r["category"] == "GREEN")
    (out / "summary.json").write_text(json.dumps({
        "command": json.loads(config.getoption("--harness-command")),
        "total": len(reports), "green": green,
        "categories": sorted({r["category"] for r in reports}),
        "scenarios": reports}, indent=2) + "\n")
    reporter = config.pluginmanager.getplugin("terminalreporter")
    if reporter:
        reporter.write_line("")
        reporter.write_line("riemann-go trial summary")
        for r in reports:
            reporter.write_line(f"  {r['scenario']:<48} {r['category']}")
        reporter.write_line(f"  -- {green}/{len(reports)} green; summary at {out / 'summary.json'}")


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
