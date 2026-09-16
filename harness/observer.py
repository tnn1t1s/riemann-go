"""Build the normalized trace from sink-receiver records and harness events.

The observer is the only piece of the harness that knows the shape of an ntfy
publish or an InfluxDB write. It translates those into trace vocabulary v0,
defined in HARNESS.md. The matcher reads the normalized trace and nothing else.

Two sources of evidence, distinguished by the `source` field:

  sink-receiver   something arrived at an external process. The implementation
                  under test cannot fabricate these.
  harness         the implementation answered when asked. Evidence about the
                  surface, not about the world. Used sparingly.
"""

import json
from typing import Any, Dict, List, Optional

from harness import sinks

# Tie-break order for events sharing a timestamp. Lower sorts first.
_NAME_PRIORITY = {
    "ingest_response": 0,
    "rule_response": 1,
    "query_response": 2,
    "influx_write": 3,
    "ntfy_post": 4,
}

# Provenance keys, per SPEC.md "Alert shape (normative)". They ride inside the
# ntfy `message` body, on a line beginning with the marker below, because the
# ntfy server discards top-level fields it does not recognize. The marker and
# the key set are pinned verbatim by the spec, so lifting them is a mechanical
# read and not an interpretation of prose.
_PROVENANCE_MARKER = "riemann-go: "

_PROVENANCE_KEYS = (
    "rule",
    "version",
    "owner",
    "host",
    "service",
    "state",
    "metric",
    "prior_state",
    "node",
)


def ntfy_provenance(body: Dict[str, Any]) -> Dict[str, Any]:
    """Lift the provenance fields out of a decoded ntfy publish body.

    This function follows SPEC.md's alert shape and is the only place that
    changes if the placement of those fields moves. A publish carrying no
    marker line yields every key as None, so a scenario asserting on
    provenance fails rather than passing on absent evidence.
    """
    found: Dict[str, Any] = {}
    message = body.get("message")
    if isinstance(message, str):
        for line in message.splitlines():
            if line.startswith(_PROVENANCE_MARKER):
                try:
                    parsed = json.loads(line[len(_PROVENANCE_MARKER):])
                except ValueError:
                    break
                if isinstance(parsed, dict):
                    found = parsed
                break
    return {k: found.get(k) for k in _PROVENANCE_KEYS}


def _ntfy_event(rec: Dict[str, Any]) -> Dict[str, Any]:
    body = sinks.parse_json_body(rec.get("body", ""))
    ev: Dict[str, Any] = {
        "ts": float(rec["ts"]),
        "source": "sink-receiver",
        "event": "ntfy_post",
        "topic": body.get("topic"),
        "title": body.get("title"),
        "message": body.get("message"),
        "priority": body.get("priority"),
        "tags": body.get("tags"),
        "raw": rec.get("body", ""),
    }
    ev.update(ntfy_provenance(body))
    return ev


def _influx_events(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One trace event per line of the write body.

    `host` and `service` are read from line-protocol tags. The measurement is
    carried separately rather than standing in for the service, so a scenario
    asserts on whichever the implementation actually wrote and never on a value
    the observer substituted.
    """
    out: List[Dict[str, Any]] = []
    for line in sinks.parse_line_protocol(rec.get("body", "")):
        tags = line["tags"]
        fields = line["fields"]
        out.append(
            {
                "ts": float(rec["ts"]),
                "source": "sink-receiver",
                "event": "influx_write",
                "measurement": line["measurement"],
                "host": tags.get("host"),
                "service": tags.get("service"),
                "state": tags.get("state"),
                "metric": fields.get("metric"),
                "time": line["timestamp"],
                "org": rec.get("query", {}).get("org"),
                "bucket": rec.get("query", {}).get("bucket"),
                "precision": rec.get("query", {}).get("precision"),
                "raw": line["raw"],
            }
        )
    return out


def events_from_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rec in records:
        if rec.get("sink") == "influx":
            out.extend(_influx_events(rec))
        else:
            out.append(_ntfy_event(rec))
    return out


def ingest_response_event(
    ts: float, status: int, accepted: Optional[int], rejected: Optional[int]
) -> Dict[str, Any]:
    return {
        "ts": float(ts),
        "source": "harness",
        "event": "ingest_response",
        "status": status,
        "accepted": accepted,
        "rejected": rejected,
    }


def rule_response_event(
    ts: float,
    kind: str,
    rule_id: str,
    status: int,
    version: Optional[Any],
    fired: Optional[bool] = None,
) -> Dict[str, Any]:
    ev = {
        "ts": float(ts),
        "source": "harness",
        "event": "rule_response",
        "kind": kind,
        "id": rule_id,
        "status": status,
        "version": version,
    }
    # `fired` is set only on a `get`, from the counters SPEC.md's rule read
    # endpoint returns. It classifies a failure and no scenario asserts on it:
    # a rule firing into the index reaches no external process, so this is the
    # only honest way to tell "no rule fired" from "no sink was reached".
    if fired is not None:
        ev["fired"] = bool(fired)
    return ev


def query_response_event(ts: float, kind: str, **fields: Any) -> Dict[str, Any]:
    ev = {
        "ts": float(ts),
        "source": "harness",
        "event": "query_response",
        "kind": kind,
    }
    ev.update(fields)
    return ev


def build_trace(
    records: List[Dict[str, Any]], harness_events: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Merge both sources, sort by (ts, event-name priority), assign seq.

    `seq` is assigned here and nowhere else, so `order` assertions are
    deterministic even when two events share a timestamp.
    """
    raw = events_from_records(records) + list(harness_events)
    raw.sort(key=lambda e: (e.get("ts", 0.0), _NAME_PRIORITY.get(e.get("event"), 99)))
    for i, ev in enumerate(raw, start=1):
        ev["seq"] = i
    return raw
