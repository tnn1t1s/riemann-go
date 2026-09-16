"""Surface adapter for the `riemannd` binary.

Ground truth for everything in this file is SPEC.md's CLI and HTTP sections,
both normative. This file is the only place in the harness that names a flag,
a path or a port; if the spec's surface moves, it moves here and nowhere
else.

The adapter knows surface only. `ttl`, `state`, `metric`, a rule's combinator
tree, a throttle limit: all opaque, forwarded exactly as the scenario wrote
them.
"""

import os
import subprocess
from typing import Any, Dict, List, Optional

import requests


class Adapter:
    name = "riemannd"

    def __init__(
        self,
        binary: str,
        listen_addr: str,
        ntfy_url: str,
        ntfy_topic: str,
        influx_url: str,
        log_path: str,
        state_dir: str,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.binary = binary
        self.listen_addr = listen_addr
        self.ntfy_url = ntfy_url
        self.ntfy_topic = ntfy_topic
        self.influx_url = influx_url
        self.log_path = log_path
        self.state_dir = state_dir
        # SPEC.md: --rules is a path to a JSON file holding an array of rule
        # documents, loaded at startup. The harness seeds the scenario's rules
        # over PUT /rules/{id} instead, so this file starts empty.
        self.rules_path = os.path.join(state_dir, "rules.json")
        # A scenario's `config:` block, passed through as `--set key=value`,
        # per SPEC.md's CLI section. The names are SCALE.md's parameters. The
        # adapter does not know what any of them mean and must not learn: it
        # renders key and value as text and hands them over.
        self.config = dict(config or {})
        self._proc: Optional[subprocess.Popen] = None
        self._log_file = None

    # ---- process ---------------------------------------------------------

    def start_command(self) -> List[str]:
        # Every URL, port and token appears here and only here, which is what
        # SPEC.md's implementation guidance requires of cmd/riemannd.
        argv = [
            self.binary,
            "--listen", self.listen_addr,
            "--rules", self.rules_path,
            "--ntfy-url", self.ntfy_url,
            "--ntfy-topic", self.ntfy_topic,
            "--influx-url", self.influx_url,
            "--influx-org", "arena",
            "--influx-bucket", "arena",
        ]
        for key, value in self.config.items():
            argv += ["--set", f"{key}={value}"]
        return argv

    def start(self) -> subprocess.Popen:
        self._log_file = open(self.log_path, "a+")
        self._proc = subprocess.Popen(
            self.start_command(), stdout=self._log_file, stderr=subprocess.STDOUT
        )
        return self._proc

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)
        self._proc = None
        if self._log_file:
            self._log_file.close()
            self._log_file = None

    def _base(self) -> str:
        return f"http://{self.listen_addr}"

    def ready(self) -> bool:
        # SPEC.md's HTTP surface pins `GET /healthz` returning 200 as the
        # readiness claim. The adapter asks that and nothing else.
        try:
            r = requests.get(f"{self._base()}/healthz", timeout=2)
        except requests.RequestException:
            return False
        return r.status_code == 200

    # ---- surface ---------------------------------------------------------

    def post_events(self, events: List[Dict[str, Any]]) -> requests.Response:
        """POST a batch. The body is the scenario's event list, unmodified."""
        return requests.post(f"{self._base()}/events", json=events, timeout=10)

    def put_rule(self, rule_id: str, body: Dict[str, Any]) -> requests.Response:
        return requests.put(f"{self._base()}/rules/{rule_id}", json=body, timeout=10)

    def get_rule(self, rule_id: str) -> requests.Response:
        return requests.get(f"{self._base()}/rules/{rule_id}", timeout=10)

    def delete_rule(self, rule_id: str) -> requests.Response:
        return requests.delete(f"{self._base()}/rules/{rule_id}", timeout=10)

    def query_index(self, expr: str) -> requests.Response:
        return requests.get(f"{self._base()}/index", params={"q": expr}, timeout=10)
