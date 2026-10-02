Feature: stable-releases-buffer-and-elides-spike

  @stable @P12 @holdout
  Scenario: stable-releases-buffer-and-elides-spike
    Source: upstream `stable-test`, the "ignores spikes" case in
    riemann/test/riemann/streams_test.clj, translated to wall-clock windows.
    Asserts SEMANTICS.md `stable` in full: the first event for a key is buffered
    rather than passed, the buffer is released when the watched value has held
    for `duration_seconds` and the releasing event is included in the release,
    and a value change discards the buffer so the events in it never reach a
    sink.

    No scenario in the development corpus drives `stable` at all; HARNESS.md
    names it as one that would use a 3 s window when written.

    Every event carries a distinct `metric`, because the property is about which
    events are emitted and which are destroyed, not how many. The two `warning`
    events are the spike: they are buffered, then discarded when the value
    returns to `ok`, and they must never appear at the oracle.

    Expected wall clock: about 9.4 s of stimulus plus a 4 s settle, so a little
    under 14 s.

    Given a settle window of 4 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "stall-detector",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"stable.probe\"",
          "stream": {
            "op": "by",
            "fields": [
              "host",
              "service"
            ],
            "children": [
              {
                "op": "stable",
                "duration_seconds": 3,
                "field": "state",
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
          "host": "ghost",
          "service": "stable.probe",
          "state": "ok",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 0.8s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "stable.probe",
          "state": "ok",
          "metric": 2,
          "ttl": 300
        }
      ]
      """
    When at 3.4s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "stable.probe",
          "state": "ok",
          "metric": 3,
          "ttl": 300
        }
      ]
      """
    When at 4.2s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "stable.probe",
          "state": "warning",
          "metric": 4,
          "ttl": 300
        }
      ]
      """
    When at 5s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "stable.probe",
          "state": "warning",
          "metric": 5,
          "ttl": 300
        }
      ]
      """
    When at 6s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "stable.probe",
          "state": "ok",
          "metric": 6,
          "ttl": 300
        }
      ]
      """
    When at 9.4s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "stable.probe",
          "state": "ok",
          "metric": 7,
          "ttl": 300
        }
      ]
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ntfy_post",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "metric": 3
        },
        {
          "event": "ntfy_post",
          "metric": 6
        },
        {
          "event": "ntfy_post",
          "metric": 7
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "state": "warning"
        }
      ]
      """
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "ntfy_post",
            "metric": 3
          },
          "after": {
            "event": "ntfy_post",
            "metric": 6
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
            "service": "stable.probe"
          },
          "equals": 5
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "service": "stable.probe"
          },
          "field": "node"
        }
      ]
      """
