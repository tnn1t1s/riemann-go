Feature: throttle-window-reopens-after-boundary

  BURNED from the held-out set on 2026-09-17, under scenarios/holdout/README.md
  rule 5. Its failure against the first generation showed that SPEC.md property
  9 described no reproducible window at all, and the wording was rewritten to
  the first-event-anchored tumbling window. The spec is now fitted to this case,
  so it measures nothing about generalisation and belongs here.

  scenarios/holdout/throttle-window-anchors-on-the-first-event.yaml holds out the
  same property, derived independently from upstream's part-time-simple-test.

  @P9
  Scenario: throttle-window-reopens-after-boundary
    Source: upstream `throttle-test` in riemann/test/riemann/streams_test.clj,
    translated, and SEMANTICS.md `throttle` ("Timing" and the first two edge
    cases). Asserts SPEC.md property 9 across a window boundary rather than
    within one window.

    The development corpus's `throttle-bounds-alerts` bounds the total count
    over a run and asserts nothing about which events pass. That passes against
    an implementation whose window never closes, as long as the ceiling is above
    `limit`. This scenario names the individual events instead: the first two of
    a burst pass, the rest of that burst are discarded rather than deferred, and
    a second burst after the window has closed gets a fresh allowance.

    Every event carries a distinct `metric`, so each assertion is about one
    specific event rather than about a total.

    The result is the same under the per-key window SEMANTICS.md states and
    under the sliding window SPEC.md property 9 reads as, which is deliberate:
    open question 2 is unresolved and this scenario must not depend on its
    answer.

    Expected wall clock: about 3.2 s of stimulus plus a 4 s settle.

    Given a settle window of 4 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "burst-bound",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"flap.probe\"",
          "stream": {
            "op": "throttle",
            "limit": 2,
            "window_seconds": 2,
            "children": [
              {
                "sink": "ntfy"
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
          "service": "flap.probe",
          "state": "s1",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 0.2s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "flap.probe",
          "state": "s2",
          "metric": 2,
          "ttl": 300
        }
      ]
      """
    When at 0.4s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "flap.probe",
          "state": "s3",
          "metric": 3,
          "ttl": 300
        }
      ]
      """
    When at 0.6s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "flap.probe",
          "state": "s4",
          "metric": 4,
          "ttl": 300
        }
      ]
      """
    When at 2.8s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "flap.probe",
          "state": "s5",
          "metric": 5,
          "ttl": 300
        }
      ]
      """
    When at 3s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "flap.probe",
          "state": "s6",
          "metric": 6,
          "ttl": 300
        }
      ]
      """
    When at 3.2s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "flap.probe",
          "state": "s7",
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
          "service": "flap.probe",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "service": "flap.probe",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "service": "flap.probe",
          "metric": 5
        },
        {
          "event": "ntfy_post",
          "service": "flap.probe",
          "metric": 6
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
          "metric": 7
        }
      ]
      """
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "ntfy_post",
            "metric": 2
          },
          "after": {
            "event": "ntfy_post",
            "metric": 5
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
            "service": "flap.probe"
          },
          "equals": 4
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "service": "flap.probe"
          },
          "field": "node"
        }
      ]
      """
