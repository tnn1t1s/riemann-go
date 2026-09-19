# Throwaway shim so riemann-surface can talk to riemann-go today.
# Not product code. It exists to show what the dashboard cutover actually
# needs, which is three small things and nothing structural:
#   1. the dashboard asks for /index?query=..., riemann-go serves /subscribe?q=
#   2. the dashboard parses `time` with Date.parse, so it needs an ISO string
#      where riemann-go sends float seconds
#   3. saved queries are in the retired grammar
import json, re, urllib.parse, urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = "http://127.0.0.1:5560"

def like_to_regex(m):
    """`=~ "ntfy.listen %"` is SQL LIKE. expr has no LIKE, so it becomes a
    regex: % is any run of characters, _ is one, everything else literal."""
    field, pat = m.group(1), m.group(2)
    out = []
    for ch in pat:
        if ch == "%": out.append(".*")
        elif ch == "_": out.append(".")
        elif ch in r'.^$*+?()[]{}|\\': out.append("\\" + ch)
        else: out.append(ch)   # not re.escape: it escapes spaces, which RE2 rejects
    # The regex lives inside an expr string literal, which consumes one level
    # of backslash, so each one is doubled. SEMANTICS.md's syntax table shows
    # the same: matches "^agent\\." for a literal dot.
    rx = "".join(out).replace("\\", "\\\\")
    return f'{field} matches "^{rx}$"'

def to_expr(q):
    """Translate a riemann-query string into an expr expression."""
    q = re.sub(r'(\w+)\s*=~\s*"([^"]*)"', like_to_regex, q)
    q = re.sub(r'(\w+)\s*~=\s*"([^"]*)"', r'\1 matches "\2"', q)
    q = re.sub(r'tagged\s+"([^"]+)"', r'tagged("\1")', q)
    q = re.sub(r'(?<![=!<>~])\s=\s(?!=)', ' == ', q)
    q = re.sub(r'\band\b', '&&', q)
    q = re.sub(r'\bor\b', '||', q)
    return q

def iso(t):
    try:
        return datetime.fromtimestamp(float(t), timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return t

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path != "/index":
            self.send_error(404); return
        qs = urllib.parse.parse_qs(u.query)
        expr = to_expr(qs.get("query", ["true"])[0])
        url = f"{UPSTREAM}/subscribe?snapshot=true&q=" + urllib.parse.quote(expr)
        print(f"  {qs.get('query',[''])[0]!r} -> {expr!r}", flush=True)

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        try:
            with urllib.request.urlopen(url, timeout=600) as r:
                for raw in r:
                    line = raw.decode("utf-8", "replace").rstrip("\n")
                    if line.startswith("data: "):
                        try:
                            ev = json.loads(line[6:])
                            ev["time"] = iso(ev.get("time"))
                            line = "data: " + json.dumps(ev)
                        except Exception:
                            pass
                    elif line.startswith("event: "):
                        continue          # the dashboard wants unnamed frames
                    self.wfile.write((line + "\n").encode())
                    self.wfile.flush()
        except Exception as e:
            print("  stream ended:", e, flush=True)

print("shim on 127.0.0.1:5561 -> riemann-go on 5560", flush=True)
ThreadingHTTPServer(("127.0.0.1", 5561), Handler).serve_forever()
