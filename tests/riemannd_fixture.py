"""A canned stand-in for riemannd, for harness self-tests only.

It speaks the subset of SPEC.md's surface the harness drives and routes
events to the sinks a rule names at its top-level leaf. It implements no
combinator and is not evidence about any generated riemannd. Modes:

  good     route every event to the rule's sink with spec-shaped bodies
  wrong    as good, with the wrong state in every ntfy post
  silent   accept everything and reach no sink
  exit     exit nonzero at startup, before binding a port
  noprov   as good, with no provenance line in the ntfy message
"""
import argparse
import json
import sys
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

p = argparse.ArgumentParser()
for flag in ("--listen", "--rules", "--ntfy-url", "--ntfy-topic", "--influx-url", "--influx-org", "--influx-bucket"):
    p.add_argument(flag, required=True)
p.add_argument("--set", action="append", default=[])
p.add_argument("--mode", default="good")
a = p.parse_args()
if a.mode == "exit":
    sys.exit(3)

RULES = {}
COUNTERS = {}
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def post(url, body, content_type):
    req = urllib.request.Request(url, data=body.encode(), method="POST",
                                 headers={"Content-Type": content_type})
    try:
        with OPENER.open(req, timeout=5):
            pass
    except urllib.error.HTTPError:
        pass


def deliver(rule_id, rule, event):
    sink = (rule.get("stream") or {}).get("sink")
    if sink not in ("ntfy", "influx"):
        return
    COUNTERS[rule_id] = COUNTERS.get(rule_id, 0) + 1
    if a.mode == "silent":
        return
    state = "wrong" if a.mode == "wrong" else event.get("state", "")
    if sink == "influx":
        line = f'{event["service"]},host={event["host"]},state={state} metric={float(event.get("metric", 0))} {time.time_ns()}'
        post(f'{a.influx_url}/api/v2/write?org={a.influx_org}&bucket={a.influx_bucket}&precision=ns',
             line + "\n", "text/plain; charset=utf-8")
        return
    prov = {"rule": rule_id, "version": rule["version"], "owner": rule.get("owner"),
            "host": event["host"], "service": event["service"], "state": state,
            "metric": event.get("metric"), "prior_state": None, "node": "stream"}
    message = f'{event["host"]} {event["service"]} is {state}'
    if a.mode != "noprov":
        message += "\nriemann-go: " + json.dumps(prov, separators=(",", ":"))
    body = {"topic": a.ntfy_topic, "title": f'{event["host"]} {event["service"]} {state}',
            "message": message, "priority": 3, "tags": [f"rule:{rule_id}", f"owner:{rule.get('owner')}"]}
    post(a.ntfy_url + "/", json.dumps(body), "application/json")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def reply(self, status, obj=None):
        payload = b"" if obj is None else json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length)) if length else None

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/healthz":
            return self.reply(200, {"ok": True})
        if url.path.startswith("/rules/"):
            rule_id = url.path[len("/rules/"):]
            if rule_id not in RULES:
                return self.reply(404, {"error": "unknown rule"})
            return self.reply(200, dict(RULES[rule_id], counters={"stream": COUNTERS.get(rule_id, 0)}))
        if url.path == "/index":
            q = parse_qs(url.query).get("q", [""])[0]
            return self.reply(200, {"as_of": {"min": time.time(), "max": time.time()},
                                    "entries": [{"host": "fixture", "service": "fixture"}] if q else []})
        self.reply(404, {"error": "no such path"})

    def do_PUT(self):
        url = urlparse(self.path)
        if not url.path.startswith("/rules/"):
            return self.reply(404, {"error": "no such path"})
        rule_id = url.path[len("/rules/"):]
        rule = dict(self.body() or {}, version=RULES.get(rule_id, {}).get("version", 0) + 1)
        RULES[rule_id] = rule
        self.reply(201, rule)

    def do_DELETE(self):
        url = urlparse(self.path)
        rule_id = url.path[len("/rules/"):]
        if RULES.pop(rule_id, None) is None:
            return self.reply(404, {"error": "unknown rule"})
        self.reply(204)

    def do_POST(self):
        url = urlparse(self.path)
        if url.path == "/events":
            events = self.body()
            events = events if isinstance(events, list) else [events]
            for event in events:
                for rule_id, rule in list(RULES.items()):
                    deliver(rule_id, rule, event)
            return self.reply(202, {"accepted": len(events), "sinks": {}})
        if url.path.startswith("/rules/") and url.path.endswith("/dryrun"):
            return self.reply(200, {"firings": []})
        self.reply(404, {"error": "no such path"})


host, port = a.listen.rsplit(":", 1)
server = ThreadingHTTPServer((host or "127.0.0.1", int(port)), Handler)
server.daemon_threads = True
server.serve_forever()
