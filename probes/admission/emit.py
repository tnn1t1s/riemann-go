import json, time, urllib.request, urllib.error
def emit(ev, url="http://127.0.0.1:18085/events"):
    req = urllib.request.Request(url, json.dumps(ev).encode(), {"Content-Type": "application/json"})
    try:
        return urllib.request.urlopen(req).status
    except urllib.error.HTTPError as e:
        if e.code != 429: raise
        time.sleep(float(e.headers.get("Retry-After", "1")))
        return urllib.request.urlopen(req).status
