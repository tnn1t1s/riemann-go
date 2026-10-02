"""Run the actual BDD runner against the canned riemannd stand-in, including failures."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/riemannd_fixture.py"


def invoke(tmp_path, mode="good", feature=None, validate=False, expression=None, holdout=False):
    env = dict(os.environ, RIEMANN_HARNESS_FEATURES=str(feature or ROOT / "tests/features"),
               RIEMANN_GO_HOLDOUT="1" if holdout else "0")
    args = [sys.executable, "-m", "pytest", "features/test_features.py", "-q", "-p", "no:cacheprovider",
            "--harness-out", str(tmp_path / "reports"), "--harness-ready-timeout", "3"]
    if validate:
        args.append("--harness-validate")
    else:
        args += ["--harness-command", json.dumps([sys.executable, str(FIXTURE), "--mode", mode]),
                 "--riemann-unbound"]
    if expression:
        args += ["-m", expression]
    result = subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True, timeout=60)
    reports = [json.loads(p.read_text()) for p in (tmp_path / "reports").glob("*/report.json")]
    return result, reports


@pytest.mark.parametrize("mode,category", [
    ("good", "GREEN"), ("wrong", "predicate_violation"),
    ("silent", "sink_error"), ("noprov", "predicate_violation"), ("exit", "start_error"),
])
def test_verdicts_follow_the_sink(tmp_path, mode, category):
    result, reports = invoke(tmp_path, mode)
    assert (result.returncode == 0) == (category == "GREEN"), result.stdout + result.stderr
    assert len(reports) == 1
    assert reports[0]["category"] == category
    assert reports[0]["build_binding"]["status"] == "unbound"
    assert len(reports[0]["feature_sha256"]) == 64
    out = Path(reports[0]["scenario"] and (tmp_path / "reports"))
    case = next(out.glob("*/"))
    assert (case / "trace.jsonl").exists() and (case / "candidate.log").exists()
    assert (tmp_path / "reports/summary.json").exists()
    summary = json.loads((tmp_path / "reports/summary.json").read_text())
    assert summary["green"] == (1 if category == "GREEN" else 0)


@pytest.mark.parametrize("mode,category", [("good", "GREEN"), ("renamed", "driver_error")])
def test_index_reads_only_the_specified_shape(tmp_path, mode, category):
    """A candidate that renames `entries` fails, rather than having its field name read."""
    result, reports = invoke(tmp_path, mode, feature=ROOT / "tests/fixtures/index-shape.feature")
    assert (result.returncode == 0) == (category == "GREEN"), result.stdout + result.stderr
    assert [r["category"] for r in reports] == [category]
    if category != "GREEN":
        # The shape that arrived is in the trace, not in report["error"]: the
        # missing final assertion overwrites that field afterwards.
        rows = [json.loads(line) for line
                in (tmp_path / "reports").glob("*/trace.jsonl").__next__().read_text().splitlines()]
        errors = [r["error"] for r in rows if r.get("event") == "run_error"]
        assert any('"entries"' in e and '"events"' in e for e in errors), errors


@pytest.mark.parametrize("mutation", ["undefined", "empty", "missing_then", "late_undefined"])
def test_invalid_features_fail_closed(tmp_path, mutation):
    text = (ROOT / "tests/features/fixture.feature").read_text()
    if mutation == "undefined":
        text = text.replace("When at 0s the emitter posts:", "When a nonexistent step is invoked:")
    elif mutation == "empty":
        text = "Feature: Empty\n  Scenario: No evidence\n    Given a settle window of 0.1 seconds\n"
    elif mutation == "missing_then":
        text = text[:text.index("    Then")]
    else:
        text += "    Then a nonexistent final step\n"
    feature = tmp_path / "invalid.feature"
    feature.write_text(text)
    result, reports = invoke(tmp_path, feature=feature, validate=mutation != "late_undefined")
    assert result.returncode != 0, result.stdout
    assert all(not r["ok"] for r in reports)


def test_holdout_is_collected_only_on_request(tmp_path):
    result, reports = invoke(tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert [r["scenario_name"] for r in reports] == ["one event routed to ntfy arrives with provenance"]
    result, reports = invoke(tmp_path / "with", holdout=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(reports) == 2
    gated = next(r for r in reports if r["scenario_name"].startswith("a held-out case"))
    assert gated["category"] == "GREEN"


def test_tag_selection_and_empty_selection(tmp_path):
    result, reports = invoke(tmp_path, expression="P1")
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(reports) == 1
    result, reports = invoke(tmp_path / "none", expression="P17")
    assert result.returncode == 5
    assert not reports


def test_skipped_case_cannot_pass(tmp_path):
    text = (ROOT / "tests/features/fixture.feature").read_text().replace("@P1", "@P1 @skipme")
    feature = tmp_path / "skip.feature"
    feature.write_text(text)
    conftest = tmp_path / "conftest.py"
    conftest.write_text("")
    env = dict(os.environ, RIEMANN_HARNESS_FEATURES=str(feature), RIEMANN_GO_HOLDOUT="0")
    args = [sys.executable, "-m", "pytest", "features/test_features.py", "-q", "-p", "no:cacheprovider",
            "--harness-out", str(tmp_path / "reports"), "--riemann-unbound",
            "--harness-command", json.dumps([sys.executable, str(FIXTURE)]),
            "-o", "markers=skipme: fixture\nP1: fixture",
            "-p", "tests.skip_plugin"]
    result = subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "1 skipped" in result.stdout
