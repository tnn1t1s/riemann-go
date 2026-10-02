Feature: backpressure-sheds-and-counts

  @P2 @P16
  Scenario: backpressure-sheds-and-counts
    A slow sink alone never produces a 429, because the shard loop never blocks
    on a sink. It sheds at the sink queue, counts every drop, and the accounting
    identity stays exact. INVARIANTS.md I3, I4 and the accounting identity in
    SCALE.md, and the observation in SCALE.md: with a sink sleeping 50 ms per
    event, ten thousand events got a hundred replies of 202 and the sink dropped
    8,998 of them.

    The harness makes the ntfy sink slow before the flood. That control is an
    in-process method call on the sink receiver, not an HTTP endpoint, so the
    code under test cannot reach it.

    Given riemannd is configured with:
      """
      {
        "sink.ntfy.queue_capacity": 32,
        "self.interval": 1
      }
      """
    Given a settle window of 10 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "alert-all",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"flood.event\"",
          "stream": {
            "sink": "ntfy"
          }
        },
        {
          "id": "shed-detector",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"riemann.sink.ntfy.dropped\" && metric > 0",
          "stream": {
            "op": "throttle",
            "limit": 1,
            "window_seconds": 60,
            "children": [
              {
                "sink": "influx"
              }
            ]
          }
        },
        {
          "id": "accounting-violation",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"riemann.accounting.residual\" && metric != 0",
          "stream": {
            "sink": "ntfy"
          }
        }
      ]
      """
    When at 0s the ntfy sink delays each reply by 0.1 seconds
    When at 0.5s the emitter posts 2000 events in batches of 100 every 0.05s from the template:
      """
      {
        "host": "host-{i}",
        "service": "flood.event",
        "state": "ok",
        "metric": 1,
        "ttl": 300
      }
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "influx_write",
          "measurement": "riemann.sink.ntfy.dropped"
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ingest_response",
          "status": 429
        },
        {
          "event": "ntfy_post",
          "rule": "accounting-violation"
        }
      ]
      """
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "ingest_response",
            "status": 202
          },
          "after": {
            "event": "influx_write",
            "measurement": "riemann.sink.ntfy.dropped"
          }
        }
      ]
      """
    Then the recorded trace has these counts:
      """
      [
        {
          "match": {
            "event": "ingest_response",
            "status": 202
          },
          "equals": 20
        },
        {
          "match": {
            "event": "ntfy_post",
            "rule": "alert-all"
          },
          "min": 1,
          "max": 150
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "influx_write",
            "measurement": "riemann.sink.ntfy.dropped"
          },
          "field": "metric"
        }
      ]
      """
