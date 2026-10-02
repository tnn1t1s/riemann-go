"""Malformed grading input fails before a candidate is launched."""
import copy

import pytest

from harness.session import RiemannSession

validate = RiemannSession.validate

GOOD = {"name": "p", "settle": 0.1,
        "rules": [{"id": "r", "stream": {"sink": "ntfy"}}],
        "steps": [{"op": "emit", "at": 0, "events": [{"host": "h", "service": "s"}]}],
        "expect": {"trace": {"contains": [{"event": "ntfy_post"}]}}}


def test_good_plan_validates():
    assert validate(copy.deepcopy(GOOD)) == GOOD


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(expect={"trace": {}}),
    lambda s: s.update(expect={"trace": {"contians": [{"event": "x"}]}}),
    lambda s: s.update(expect={"trace": {"contains": [{}]}}),
    lambda s: s.update(expect={"trace": {"count": [{"match": {"event": "x"}}]}}),
    lambda s: s.update(expect={"trace": {"count": [{"match": {"event": "x"}, "min": 3, "max": 1}]}}),
    lambda s: s.update(expect={"trace": {"count": [{"match": {"event": "x"}, "exact": 1}]}}),
    lambda s: s.update(expect={"trace": {"order": [{"before": {"event": "x"}}]}}),
    lambda s: s.update(expect={"trace": {"field_exists": [{"match": {"event": "x"}}]}}),
    lambda s: s.update(expect={"log": "never"}),
    lambda s: s["steps"].append({"op": "sleep", "at": 1}),
    lambda s: s["steps"].append({"op": "emit", "at": -1, "events": [{"host": "h"}]}),
    lambda s: s["steps"].append({"op": "emit", "at": 0, "events": []}),
    lambda s: s["steps"].append({"op": "sink_delay", "at": 0, "sink": "pager", "seconds": 1}),
    lambda s: s["steps"].append({"op": "emit_n", "at": 0, "count": 0, "batch_size": 1, "interval": 0, "template": {"host": "h"}}),
    lambda s: s.update(rules=[{"owner": "no-id"}]),
    lambda s: s.update(config={"shard.inbox_capacity": [8]}),
    lambda s: s.update(settle=-1),
    lambda s: s.update(rules=[], steps=[]),
    lambda s: s.update(name=""),
])
def test_invalid_plans_are_rejected(mutate):
    s = copy.deepcopy(GOOD)
    mutate(s)
    with pytest.raises(ValueError):
        validate(s)
