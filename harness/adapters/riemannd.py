"""Surface adapter for the `riemannd` binary.

Ground truth for everything in this file is SPEC.md's CLI and HTTP sections,
both normative. This file is the only place in the harness that names a flag,
a path or a port; if the spec's surface moves, it moves here and nowhere
else.

The adapter knows surface only. `ttl`, `state`, `metric`, a rule's combinator
tree, a throttle limit: all opaque, forwarded exactly as the scenario wrote
them. Launch, readiness probing and stopping the process group come from the
shared adapter.
"""
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from riemann_harness.adapter import Adapter as BaseAdapter
from riemann_harness.http import request

# One request to the candidate's surface, in seconds. Default: a 500-event
# batch to a loopback riemannd answers in milliseconds; ten leaves room for a
# candidate under the flood scenarios without hiding a hang behind the settle.
REQUEST_TIMEOUT_SECONDS = 10


class Adapter(BaseAdapter):
    name = "riemannd"

    def __init__(self, command: List[str], listen: str, directory: str,
                 ntfy_url: str, ntfy_topic: str, influx_url: str,
                 config: Optional[Dict[str, Any]] = None):
        # SPEC.md's HTTP surface pins `GET /healthz` returning 200 as the
        # readiness claim. The adapter asks that and nothing else.
        super().__init__(command, listen, directory, ready_path="/healthz")
        self.ntfy_url = ntfy_url
        self.ntfy_topic = ntfy_topic
        self.influx_url = influx_url
        # SPEC.md: --rules is a path to a JSON file holding an array of rule
        # documents, loaded at startup. The harness seeds the scenario's rules
        # over PUT /rules/{id} instead, so this file starts empty.
        self.rules_path = self.directory / "rules.json"
        self.rules_path.write_text("[]\n")
        # A scenario's configuration block, passed through as `--set key=value`,
        # per SPEC.md's CLI section. The names are SCALE.md's parameters. The
        # adapter does not know what any of them mean and must not learn: it
        # renders key and value as text and hands them over.
        self.config = dict(config or {})

    def flags(self) -> List[str]:
        # Every URL, port and token appears here and only here, which is what
        # SPEC.md's implementation guidance requires of cmd/riemannd.
        argv = super().flags() + [
            "--rules", str(self.rules_path),
            "--ntfy-url", self.ntfy_url,
            "--ntfy-topic", self.ntfy_topic,
            "--influx-url", self.influx_url,
            "--influx-org", "arena",
            "--influx-bucket", "arena",
        ]
        for key, value in self.config.items():
            argv += ["--set", f"{key}={value}"]
        return argv

    # ---- surface ---------------------------------------------------------
    # Each method returns (status, headers, decoded body) from an independent
    # HTTP client; a body that is not JSON decodes to its text.

    def _url(self, path: str) -> str:
        return self.base + path

    def post_events(self, events: List[Dict[str, Any]]):
        """POST a batch. The body is the scenario's event list, unmodified."""
        return request(self._url("/events"), "POST", events, timeout=REQUEST_TIMEOUT_SECONDS)[:3]

    def put_rule(self, rule_id: str, body: Dict[str, Any]):
        return request(self._url("/rules/" + quote(rule_id, safe="")), "PUT", body, timeout=REQUEST_TIMEOUT_SECONDS)[:3]

    def get_rule(self, rule_id: str):
        return request(self._url("/rules/" + quote(rule_id, safe="")), timeout=REQUEST_TIMEOUT_SECONDS)[:3]

    def dryrun_rule(self, rule_id: str, body: Dict[str, Any]):
        return request(self._url("/rules/" + quote(rule_id, safe="") + "/dryrun"), "POST", body,
                       timeout=REQUEST_TIMEOUT_SECONDS)[:3]

    def delete_rule(self, rule_id: str):
        return request(self._url("/rules/" + quote(rule_id, safe="")), "DELETE", timeout=REQUEST_TIMEOUT_SECONDS)[:3]

    def query_index(self, expr: str):
        return request(self._url("/index?q=" + quote(expr, safe="")), timeout=REQUEST_TIMEOUT_SECONDS)[:3]
