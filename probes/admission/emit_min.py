import json, urllib.request
urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:18085/events", json.dumps({"host":"ghost","service":"agent.tokens.out","metric":1234,"ttl":90}).encode(), {"Content-Type":"application/json"}))
