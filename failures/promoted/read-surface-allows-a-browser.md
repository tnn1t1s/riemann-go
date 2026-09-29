# The read surface refused a browser

Found in production on 2026-09-29, during the dashboard cutover. Encoded as
`scenarios/read-surface-allows-a-browser.yaml`.

## What happened

With the Clojure server stopped and riemann-go serving, the dashboard showed
empty panes. Its queries were correct, translated correctly, and every one of
them returned `200` to `curl`. The browser console showed only a socket error
two seconds after each subscription opened.

The server sent no `Access-Control-Allow-Origin` header. The dashboard loads
its page from one origin and subscribes to riemann-go on another, so the
browser refused every response before reading a byte. Nothing was wrong in the
server, nothing failed in its metrics, and nothing appeared in its logs.

## Why the corpus could not catch it

The harness drives the server with Python's `requests`, where the same-origin
policy does not exist. Every existing scenario would pass against a server no
browser can read.

This is the third defect found by deployment that the arena is structurally
blind to, after the InfluxDB credential (the harness sink accepts anything)
and the bus address (the harness resolves names the cluster cannot). All three
share a cause: the harness talks to fakes on the same machine as itself.

## What the scenario asserts

That a query returns its match and that the response carries the header. The
observer records `allow_origin` on `query_response`, which is a mechanical
reading of a header rather than an interpretation of anything.

## A second defect it exposed

Writing it surfaced a harness bug nobody had noticed: the runner read a
`events` key from `GET /index`, which answers `entries`, so every index query
reported zero matches. No scenario had asserted on that count, so it had never
mattered. Fixed in the same change.
