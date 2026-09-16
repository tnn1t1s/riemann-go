"""Per-implementation surface adapters.

An adapter supplies the start command, the ready check, and how to post events,
put and delete a rule, and query the index. It never interprets a combinator, a
threshold, a TTL or a state: those pass through opaque, exactly as the scenario
wrote them. If a generation ignores a field, the sink trace will show it and the
scenario will fail. That is the correct level of coupling.

There is one adapter, `riemannd`. A generation whose surface it cannot drive is
a spec violation, not an adapter gap.
"""

from importlib import import_module


def load(name: str):
    mod = import_module(f"harness.adapters.{name}")
    return mod.Adapter
