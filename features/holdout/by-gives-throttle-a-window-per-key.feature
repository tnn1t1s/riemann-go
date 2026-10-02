Feature: by-gives-throttle-a-window-per-key

  @P7 @P9 @holdout
  Scenario: by-gives-throttle-a-window-per-key
    Source: an interaction the development corpus covers nowhere. `by` is
    exercised alone and `throttle` is exercised alone, and SEMANTICS.md `by`
    ("Timing") states that a `throttle` under a `by` has one window per key.
    Asserts SPEC.md property 7 applied to property 9.

    Per-key throttle state is exactly the kind of thing one shared counter fakes
    successfully in a scenario that only counts alerts. Here two hosts
    interleave under a `throttle` of one per window, so the correct
    implementation emits the first event of each host and a single shared
    counter emits only the first event overall. The two outcomes differ in which
    alerts arrive, not only in how many.

    The window is 6 s and the stimulus is under 2 s, so no window closes while
    events are arriving and every suppression in this scenario is the limit
    doing its work rather than a boundary.

    Expected wall clock: about 2 s of stimulus plus a 5 s settle.

    Given riemannd is configured with:
      """
      {
        "engine.shards": 1
      }
      """
    Given a settle window of 5 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "per-host-bound",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"fork.probe\"",
          "stream": {
            "op": "by",
            "fields": [
              "host"
            ],
            "children": [
              {
                "op": "throttle",
                "limit": 1,
                "window_seconds": 6,
                "children": [
                  {
                    "sink": "ntfy"
                  }
                ]
              }
            ]
          }
        }
      ]
      """
    When at 0s the emitter posts:
      """
      [
        {
          "host": "alpha",
          "service": "fork.probe",
          "state": "ok",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 0.3s the emitter posts:
      """
      [
        {
          "host": "beta",
          "service": "fork.probe",
          "state": "ok",
          "metric": 2,
          "ttl": 300
        }
      ]
      """
    When at 0.8s the emitter posts:
      """
      [
        {
          "host": "alpha",
          "service": "fork.probe",
          "state": "ok",
          "metric": 3,
          "ttl": 300
        }
      ]
      """
    When at 1.1s the emitter posts:
      """
      [
        {
          "host": "beta",
          "service": "fork.probe",
          "state": "ok",
          "metric": 4,
          "ttl": 300
        }
      ]
      """
    When at 1.6s the emitter posts:
      """
      [
        {
          "host": "alpha",
          "service": "fork.probe",
          "state": "ok",
          "metric": 5,
          "ttl": 300
        }
      ]
      """
    When at 1.9s the emitter posts:
      """
      [
        {
          "host": "beta",
          "service": "fork.probe",
          "state": "ok",
          "metric": 6,
          "ttl": 300
        }
      ]
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ntfy_post",
          "host": "alpha",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "host": "beta",
          "metric": 2
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "metric": 3
        },
        {
          "event": "ntfy_post",
          "metric": 4
        },
        {
          "event": "ntfy_post",
          "metric": 5
        },
        {
          "event": "ntfy_post",
          "metric": 6
        }
      ]
      """
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "ntfy_post",
            "host": "alpha",
            "metric": 1
          },
          "after": {
            "event": "ntfy_post",
            "host": "beta",
            "metric": 2
          }
        }
      ]
      """
    Then the recorded trace has these counts:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "rule": "per-host-bound"
          },
          "equals": 2
        },
        {
          "match": {
            "event": "ntfy_post",
            "host": "alpha"
          },
          "equals": 1
        },
        {
          "match": {
            "event": "ntfy_post",
            "host": "beta"
          },
          "equals": 1
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "rule": "per-host-bound"
          },
          "field": "node"
        }
      ]
      """
