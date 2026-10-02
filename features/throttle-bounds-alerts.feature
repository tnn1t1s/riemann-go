Feature: throttle-bounds-alerts

  @P9
  Scenario: throttle-bounds-alerts
    throttle passes at most `limit` events per `window_seconds` and drops the
    rest. A stream flapping four times a second through changed-state produces
    twenty transitions; a throttle of 2 per 2 s must let a small bounded number
    through, not twenty. Ported from streams_test.clj throttle-test, and the
    shape the fleet's ntfy.listen.connected rule uses to keep a flapping
    listener reading as one burst rather than a page per flip.

    The window is 2 s on the wall clock, not the fleet's 300 s. Window length is
    a rule parameter the scenario chooses, while the combinator semantics are
    what the corpus tests, and both exercise the same throttle. See HARNESS.md
    "Time-domain scenarios use short real windows".

    The bound is asserted globally over the run rather than per window, because
    the five operators have no time arithmetic. Twenty transitions arrive over
    five seconds, so at most three windows are touched and the ceiling is 3 x
    limit. See HARNESS.md "Why there is no within_seconds".

    Given a settle window of 3 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "listener-flap",
          "owner": "ops",
          "partition": "host",
          "match": "service == \"ntfy.listen.connected\"",
          "stream": {
            "op": "changed-state",
            "initial": "ok",
            "children": [
              {
                "op": "by",
                "fields": [
                  "host",
                  "service"
                ],
                "children": [
                  {
                    "op": "throttle",
                    "limit": 2,
                    "window_seconds": 2,
                    "children": [
                      {
                        "sink": "ntfy"
                      }
                    ]
                  }
                ]
              }
            ]
          }
        }
      ]
      """
    # Twenty events at 250 ms. Every one carries a distinct state, so every one is
    # a transition: changed-state forwards all twenty and the throttle is what
    # bounds them. Five seconds of stimulus spans three 2 s windows.
    When at 0s the emitter posts 20 events in batches of 1 every 0.25s from the template:
      """
      {
        "host": "ghost",
        "service": "ntfy.listen.connected",
        "state": "{i}",
        "ttl": 300
      }
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ntfy_post",
          "host": "ghost",
          "service": "ntfy.listen.connected"
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ingest_response",
          "status": 429
        }
      ]
      """
    # The rule is registered before it can fire. The first 202 is not used as the
    # `before`, because the reply and the first alert can land in either order
    # within a millisecond of each other.
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "rule_response",
            "kind": "put",
            "id": "listener-flap"
          },
          "after": {
            "event": "ntfy_post",
            "service": "ntfy.listen.connected"
          }
        }
      ]
      """
    # Not more. Twenty transitions in, at most six out across three windows.
    Then the recorded trace has these counts:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "service": "ntfy.listen.connected"
          },
          "min": 2,
          "max": 6
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "service": "ntfy.listen.connected"
          },
          "field": "rule"
        }
      ]
      """
