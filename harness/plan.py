"""Fail closed on malformed grading input before launching a candidate.

A plan is what the Gherkin steps of one scenario assemble: configuration,
seeded rules, a timeline of stimulus steps and the assertion block. The
harness validates it before a candidate is launched and again before the
verdict, so an undefined operator, an empty assertion block or a malformed
step can never pass by being ignored.
"""
import math

OPS = {"contains", "not_contains", "order", "count", "field_exists"}
SINKS = {"ntfy", "influx"}
STEPS = {
    "emit": {"events"},
    "emit_n": {"count", "batch_size", "interval", "template"},
    "put_rule": {"id", "rule"},
    "delete_rule": {"id"},
    "dryrun_rule": {"id", "rule"},
    "query_index": {"q"},
    "sink_delay": {"sink", "seconds"},
    "sink_fail": {"sink", "status"},
}


def finite(value, zero=False):
    return (type(value) in (int, float) and not isinstance(value, bool)
            and math.isfinite(value) and (value >= 0 if zero else value > 0))


def pattern(value):
    if not isinstance(value, dict) or not value:
        raise ValueError("a match pattern must be a nonempty object")


def validate_expect(expect):
    if not isinstance(expect, dict) or set(expect) != {"trace"}:
        raise ValueError("expect must contain only trace")
    block = expect["trace"]
    if not isinstance(block, dict) or set(block) - OPS:
        raise ValueError("unknown assertion operator")
    if not block or not any(block.values()):
        raise ValueError("empty assertions cannot pass")
    for op, assertions in block.items():
        if not isinstance(assertions, list):
            raise ValueError("assertions must be arrays")
        for assertion in assertions:
            if op in ("contains", "not_contains"):
                pattern(assertion)
            elif op == "order":
                if not isinstance(assertion, dict) or set(assertion) != {"before", "after"}:
                    raise ValueError("order needs before and after")
                pattern(assertion["before"])
                pattern(assertion["after"])
            elif op == "field_exists":
                if (not isinstance(assertion, dict) or set(assertion) != {"match", "field"}
                        or not isinstance(assertion["field"], str) or not assertion["field"]):
                    raise ValueError("field_exists needs match and field")
                pattern(assertion["match"])
            else:
                if not isinstance(assertion, dict) or set(assertion) - {"match", "equals", "min", "max"}:
                    raise ValueError("unknown count keys")
                pattern(assertion.get("match"))
                bounds = set(assertion) - {"match"}
                if not bounds or any(type(assertion[k]) is not int or assertion[k] < 0 for k in bounds):
                    raise ValueError("count needs nonnegative integer bounds")
                if "min" in bounds and "max" in bounds and assertion["min"] > assertion["max"]:
                    raise ValueError("reversed count bounds")


def validate_step(step):
    if not isinstance(step, dict):
        raise ValueError("step is not an object")
    op = step.get("op")
    if op not in STEPS or set(step) - (STEPS[op] | {"op", "at"}) or not STEPS[op] <= set(step):
        raise ValueError("invalid step: " + repr(op))
    if not finite(step.get("at"), zero=True):
        raise ValueError("step needs a finite nonnegative offset")
    if op == "emit" and (not isinstance(step["events"], list) or not step["events"]
                         or not all(isinstance(e, dict) for e in step["events"])):
        raise ValueError("emit needs a nonempty array of event objects")
    if op == "emit_n":
        if (type(step["count"]) is not int or step["count"] < 1
                or type(step["batch_size"]) is not int or step["batch_size"] < 1
                or not finite(step["interval"], zero=True)
                or not isinstance(step["template"], dict) or not step["template"]):
            raise ValueError("emit_n needs count, batch_size, interval and a template")
    if op in ("put_rule", "dryrun_rule") and (not isinstance(step["rule"], dict) or not step["rule"]):
        raise ValueError(op + " needs a rule object")
    if "id" in step and (not isinstance(step["id"], str) or not step["id"]):
        raise ValueError("rule id must be a nonempty string")
    if op == "query_index" and (not isinstance(step["q"], str) or not step["q"]):
        raise ValueError("query_index needs an expression")
    if op in ("sink_delay", "sink_fail") and step["sink"] not in SINKS:
        raise ValueError("unknown sink: " + repr(step["sink"]))
    if op == "sink_delay" and not finite(step["seconds"], zero=True):
        raise ValueError("sink_delay needs finite nonnegative seconds")
    if op == "sink_fail" and step["status"] is not None and (type(step["status"]) is not int
                                                            or not 100 <= step["status"] <= 599):
        raise ValueError("sink_fail status must be an HTTP status or null")


def validate(plan):
    if not isinstance(plan, dict) or set(plan) - {"name", "config", "settle", "ntfy_topic", "rules", "steps", "expect"}:
        raise ValueError("unknown plan keys or non-object plan")
    if not isinstance(plan.get("name"), str) or not plan["name"]:
        raise ValueError("plan needs a name")
    config = plan.get("config", {})
    if not isinstance(config, dict) or not all(isinstance(k, str) and k and isinstance(v, (str, int, float))
                                               and not isinstance(v, bool) for k, v in config.items()):
        raise ValueError("config must map parameter names to scalar values")
    if not finite(plan.get("settle", 5), zero=True):
        raise ValueError("invalid settle")
    if not isinstance(plan.get("ntfy_topic", "arena"), str) or not plan.get("ntfy_topic", "arena"):
        raise ValueError("ntfy_topic must be a nonempty string")
    rules = plan.get("rules", [])
    if not isinstance(rules, list) or not all(isinstance(r, dict) and isinstance(r.get("id"), str) and r["id"]
                                              for r in rules):
        raise ValueError("rules must be an array of rule documents with ids")
    steps = plan.get("steps", [])
    if not isinstance(steps, list):
        raise ValueError("steps must be an array")
    for step in steps:
        validate_step(step)
    if not rules and not steps:
        raise ValueError("a scenario needs rules or stimulus; an empty run observes nothing")
    validate_expect(plan.get("expect"))
    return plan
