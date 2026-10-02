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
    # Self-observation is an ordinary event stream, so the drop counter is an
    # ordinary rule. The threshold lives in the rule, which is why the matcher
    # needs no comparison operator to assert "drops happened".
    #
    # shed-detector's throttle is not a changed-state. Self-observation events
    # carry state "ok" on every sample, so a changed-state here never sees a
    # transition and correctly suppresses all of them; three generations
    # implemented that faithfully and this scenario called each of them wrong. A
    # throttle gives the "tell me once" the rule wanted, keyed on arrival rather
    # than on state.
    #
    # Its sink is influx, not ntfy. This rule reports that the ntfy queue is
    # shedding, and its own alert would join that same queue and be shed with
    # everything else. An alert about a saturated sink cannot travel through it.
    #
    # accounting-violation asserts the accounting identity by its absence.
    # `riemann.accounting.residual` is the self-observation field this property
    # needs and SPEC.md must pin: it is accepted - (processed + dropped + queued +
    # in_flight), and SPEC.md invariant 6 says it is exactly zero. If SPEC.md names
    # it differently, this rule's match string changes and nothing else does.
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
    # 100 ms per ntfy post. The queue holds 32; the flood is 2000.
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
    # Drops were counted and surfaced as events, not lost silently.
    Then the recorded trace contains:
      """
      [
        {
          "event": "influx_write",
          "measurement": "riemann.sink.ntfy.dropped"
        }
      ]
      """
    # A slow sink does not saturate the loop, so no emitter sees a 429. And the
    # accounting identity held: the accounting-violation alert firing means it did
    # not.
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
    # Every batch was admitted: twenty batches of a hundred. Then the sink shed.
    # Two thousand events in, far fewer posts out: the run is about eleven seconds
    # and the oracle takes 100 ms per post, so the receiver cannot have seen more
    # than about 110 of them.
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
