"""The oracle: an HTTP server that stands in for ntfy and InfluxDB v2.

The harness owns this server. It binds 127.0.0.1 on an ephemeral port and
records every request verbatim. The implementation under test posts to it and
cannot read it, reconfigure it, or write to its record. That asymmetry is the
whole point: a generated riemann-go is graded on what an external process
observed arrive, never on what it says about itself.

Two surfaces:

  POST /api/v2/write?org=&bucket=&precision=   InfluxDB v2 line protocol
  POST <anything else>                         ntfy publish, JSON body

The ntfy shape follows `riemann/src/riemann/ntfy.clj`, which posts a JSON body
carrying topic, title, message, priority and tags to the server's base URL.

Fault injection is in-process only. `delay` and `fail` are set by the harness
through method calls, not through an HTTP control endpoint, so the code under
test has no way to reach them.
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

INFLUX_WRITE_PATH = "/api/v2/write"


class SinkState:
    """Records and fault settings, shared across handler threads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.records: List[Dict[str, Any]] = []
        # Per-sink injected latency in seconds, applied before the reply.
        self.delay: Dict[str, float] = {"ntfy": 0.0, "influx": 0.0}
        # Per-sink injected failure status, or None to reply normally.
        self.fail_status: Dict[str, Optional[int]] = {"ntfy": None, "influx": None}

    def record(self, rec: Dict[str, Any]) -> None:
        with self._lock:
            self.records.append(rec)

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.records)

    def set_delay(self, sink: str, seconds: float) -> None:
        if sink not in self.delay:
            raise ValueError(f"unknown sink: {sink!r}")
        self.delay[sink] = float(seconds)

    def set_fail(self, sink: str, status: Optional[int]) -> None:
        if sink not in self.fail_status:
            raise ValueError(f"unknown sink: {sink!r}")
        self.fail_status[sink] = status


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    @property
    def state(self) -> SinkState:
        return self.server.sink_state  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # silence stderr chatter
        pass

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler naming
        parsed = urlparse(self.path)
        sink = "influx" if parsed.path == INFLUX_WRITE_PATH else "ntfy"

        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8", errors="replace") if length else ""

        self.state.record(
            {
                "ts": time.time(),
                "sink": sink,
                "method": "POST",
                "path": parsed.path,
                "query": {k: v[0] for k, v in parse_qs(parsed.query).items()},
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": body,
            }
        )

        delay = self.state.delay.get(sink, 0.0)
        if delay > 0:
            time.sleep(delay)

        fail = self.state.fail_status.get(sink)
        if fail is not None:
            self._reply(fail, b'{"error":"injected"}')
            return
        # ntfy replies 200 with the published message; InfluxDB v2 replies 204.
        if sink == "influx":
            self._reply(204, b"")
        else:
            self._reply(200, b"{}")

    def do_GET(self):  # noqa: N802
        # Nothing under test reads from the oracle. Say so rather than 404.
        self._reply(405, b'{"error":"the sink receiver is write-only"}')

    def _reply(self, status: int, payload: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)


class SinkReceiver:
    """Lifecycle wrapper. Started and stopped by the harness, never by a test."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.state = SinkState()
        self._server = ThreadingHTTPServer((host, port), _Handler)
        self._server.sink_state = self.state  # type: ignore[attr-defined]
        self._server.daemon_threads = True
        self._thread: Optional[threading.Thread] = None

    @property
    def host(self) -> str:
        return self._server.server_address[0]

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def ntfy_url(self) -> str:
        """Base URL an ntfy client posts its JSON body to."""
        return self.base_url

    @property
    def influx_url(self) -> str:
        """Base URL an InfluxDB v2 client appends /api/v2/write to."""
        return self.base_url

    def start(self) -> "SinkReceiver":
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def records(self) -> List[Dict[str, Any]]:
        return self.state.snapshot()

    def set_delay(self, sink: str, seconds: float) -> None:
        self.state.set_delay(sink, seconds)

    def set_fail(self, sink: str, status: Optional[int]) -> None:
        self.state.set_fail(sink, status)

    def __enter__(self) -> "SinkReceiver":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()


def parse_line_protocol(body: str) -> List[Dict[str, Any]]:
    """Parse an InfluxDB line-protocol body into measurement/tags/fields/time.

    Line protocol is `measurement[,tag=v...] field=v[,field=v...] [timestamp]`,
    with commas, spaces and equals signs escaped by a backslash inside keys and
    unquoted values. Quoted string field values may contain anything.
    """
    out: List[Dict[str, Any]] = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = _split_unescaped(line, " ", quoted=True)
        if len(parts) < 2:
            continue
        head, fields_str = parts[0], parts[1]
        ts = None
        if len(parts) >= 3 and parts[2]:
            try:
                ts = int(parts[2])
            except ValueError:
                ts = None

        head_parts = _split_unescaped(head, ",")
        measurement = _unescape(head_parts[0])
        tags: Dict[str, str] = {}
        for kv in head_parts[1:]:
            k, _, v = _partition_unescaped(kv, "=")
            tags[_unescape(k)] = _unescape(v)

        fields: Dict[str, Any] = {}
        for kv in _split_unescaped(fields_str, ",", quoted=True):
            k, _, v = _partition_unescaped(kv, "=")
            fields[_unescape(k)] = _coerce_field(v)

        out.append(
            {
                "measurement": measurement,
                "tags": tags,
                "fields": fields,
                "timestamp": ts,
                "raw": line,
            }
        )
    return out


def _coerce_field(v: str) -> Any:
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        return v[1:-1].replace('\\"', '"')
    if v in ("t", "T", "true", "True", "TRUE"):
        return True
    if v in ("f", "F", "false", "False", "FALSE"):
        return False
    if v.endswith("i") or v.endswith("u"):
        try:
            return int(v[:-1])
        except ValueError:
            return v
    try:
        return float(v)
    except ValueError:
        return v


def _split_unescaped(s: str, sep: str, quoted: bool = False) -> List[str]:
    parts, buf, esc, in_q = [], [], False, False
    for ch in s:
        if esc:
            buf.append(ch)
            esc = False
            continue
        if ch == "\\":
            esc = True
            buf.append(ch)
            continue
        if quoted and ch == '"':
            in_q = not in_q
            buf.append(ch)
            continue
        if ch == sep and not in_q:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    parts.append("".join(buf))
    return parts


def _partition_unescaped(s: str, sep: str):
    parts = _split_unescaped(s, sep, quoted=True)
    if len(parts) == 1:
        return parts[0], "", ""
    return parts[0], sep, sep.join(parts[1:])


def _unescape(s: str) -> str:
    out, esc = [], False
    for ch in s:
        if esc:
            out.append(ch)
            esc = False
            continue
        if ch == "\\":
            esc = True
            continue
        out.append(ch)
    return "".join(out)


def parse_json_body(body: str) -> Dict[str, Any]:
    """Decode an ntfy publish body. A body that is not a JSON object is an
    empty dict, and the observer records the raw text alongside it, so a
    malformed post is visible in the trace rather than silently dropped."""
    try:
        v = json.loads(body)
    except Exception:
        return {}
    return v if isinstance(v, dict) else {}
