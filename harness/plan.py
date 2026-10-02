"""riemann-go's own checks on a plan, run before the shared scenario validation.

The shared validation covers step tables, timeouts and the assertion block.
What is checked here is what only riemann-go knows: the shape of its
configuration, rules and stimulus steps, and the sinks a fault can target.
"""
import math

SINKS = {"ntfy", "influx"}


def finite(value, zero=False):
    return (type(value) in (int, float) and not isinstance(value, bool)
            and math.isfinite(value) and (value >= 0 if zero else value > 0))


def validate_step(step):
    if not isinstance(step, dict):
        raise ValueError("step is not an object")
    op = step.get("op")
    if "at" in step and not finite(step["at"], zero=True):
        raise ValueError("step needs a finite nonnegative offset")
    if op == "emit" and (not isinstance(step.get("events"), list) or not step["events"]
                         or not all(isinstance(e, dict) for e in step["events"])):
        raise ValueError("emit needs a nonempty array of event objects")
    if op == "emit_n":
        if (type(step.get("count")) is not int or step["count"] < 1
                or type(step.get("batch_size")) is not int or step["batch_size"] < 1
                or not finite(step.get("interval"), zero=True)
                or not isinstance(step.get("template"), dict) or not step["template"]):
            raise ValueError("emit_n needs count, batch_size, interval and a template")
    if op in ("put_rule", "dryrun_rule") and (not isinstance(step.get("rule"), dict) or not step["rule"]):
        raise ValueError(op + " needs a rule object")
    if "id" in step and (not isinstance(step["id"], str) or not step["id"]):
        raise ValueError("rule id must be a nonempty string")
    if op == "query_index" and (not isinstance(step.get("q"), str) or not step["q"]):
        raise ValueError("query_index needs an expression")
    if op in ("sink_delay", "sink_fail") and step.get("sink") not in SINKS:
        raise ValueError("unknown sink: " + repr(step.get("sink")))
    if op == "sink_delay" and not finite(step.get("seconds"), zero=True):
        raise ValueError("sink_delay needs finite nonnegative seconds")
    if op == "sink_fail" and step.get("status") is not None and (type(step["status"]) is not int
                                                                or not 100 <= step["status"] <= 599):
        raise ValueError("sink_fail status must be an HTTP status or null")


def validate(plan):
    if not isinstance(plan, dict):
        raise ValueError("non-object plan")
    config = plan.get("config", {})
    if not isinstance(config, dict) or not all(isinstance(k, str) and k and isinstance(v, (str, int, float))
                                               and not isinstance(v, bool) for k, v in config.items()):
        raise ValueError("config must map parameter names to scalar values")
    if not isinstance(plan.get("ntfy_topic", "arena"), str) or not plan.get("ntfy_topic", "arena"):
        raise ValueError("ntfy_topic must be a nonempty string")
    rules = plan.get("rules", [])
    if not isinstance(rules, list) or not all(isinstance(r, dict) and isinstance(r.get("id"), str) and r["id"]
                                              for r in rules):
        raise ValueError("rules must be an array of rule documents with ids")
    for step in plan.get("steps") or []:
        validate_step(step)
    return plan
