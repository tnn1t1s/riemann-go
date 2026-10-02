Feature: expired-name-means-the-reaper-only

  @P4 @expression_language @holdout
  Scenario: expired-name-means-the-reaper-only
    Source: upstream `expired-test` and `not-expired-test` in
    riemann/test/riemann/streams_test.clj, plus the `(where* expired?)` case in
    `where*-test`. Adjusted, because SEMANTICS.md "Expiry", "The `expired` name"
    states that riemann-go's `expired` is a strictly narrower predicate than
    upstream's.

    Upstream's `expired?` (src/riemann/streams.clj:50-59) is true in two
    situations, and its two tests cover one each: `expired-test`'s first case
    drives events whose `state` is the string "expired", and its second case
    drives events whose `time` plus `ttl` has already passed while their state
    is ordinary. Upstream cannot tell a reaper event from a stale one. riemann-
    go's `expired` name is true for neither of those: SPEC.md's expression
    language defines it as true when the event was produced by index expiry
    rather than by ingest, and SEMANTICS.md says an author who wants both writes
    the state comparison instead.

    So this scenario drives all three kinds of event past both predicates and
    asserts which combinations fire:


    reaper event      `expired` yes, `state == "expired"` yes

    client-sent       `expired` no,  `state == "expired"` yes

    stale timestamp   `expired` no,  `state == "expired"` no


    The stale event carries a `time` in the distant past with a one-second ttl.
    HARNESS.md says scenarios do not backdate the `time` field, on the grounds
    that backdating advances no timer. That reason does not apply here: this
    event's timestamp is the data under test rather than a way to move a clock,
    and the event is never routed to an index leaf, so nothing schedules an
    expiry from it.

    Only `exp.probe` is indexed, so exactly one reaper event exists in the run.

    Wrong implementations this catches: the obvious port of upstream's
    `expired?`, where the stale event satisfies `expired` and the whole scenario
    inverts; an `expired` implemented as a synonym for `state == "expired"`,
    which makes the client-sent event fire the reaper rule; a reaper that copies
    the expiring entry wholesale, which would carry metric 1 into the expiry
    alert.

    Expected wall clock: 1 s of stimulus, a 2 s ttl, and an 8 s settle.

    Given a settle window of 8 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "archive-probe",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"exp.probe\"",
          "stream": {
            "sink": "index"
          }
        },
        {
          "id": "expired-by-name",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"exp.probe\" || service == \"exp.client\" || service == \"exp.stale\"",
          "stream": {
            "op": "where",
            "expr": "expired",
            "children": [
              {
                "op": "set",
                "fields": {
                  "service": "\"exp.byname\""
                },
                "children": [
                  {
                    "sink": "ntfy"
                  }
                ]
              }
            ]
          }
        },
        {
          "id": "expired-by-state",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"exp.probe\" || service == \"exp.client\" || service == \"exp.stale\"",
          "stream": {
            "op": "where",
            "expr": "state == \"expired\"",
            "children": [
              {
                "op": "set",
                "fields": {
                  "service": "\"exp.bystate\""
                },
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
          "service": "exp.probe",
          "state": "ok",
          "metric": 1,
          "ttl": 2
        }
      ]
      """
    When at 0.5s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "exp.client",
          "state": "expired",
          "metric": 2,
          "ttl": 300
        }
      ]
      """
    When at 1s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "exp.stale",
          "state": "ok",
          "metric": 3,
          "ttl": 1,
          "time": 1000000000.0
        }
      ]
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ntfy_post",
          "service": "exp.byname",
          "host": "ghost",
          "state": "expired",
          "metric": null
        },
        {
          "event": "ntfy_post",
          "service": "exp.bystate",
          "host": "ghost",
          "state": "expired",
          "metric": null
        },
        {
          "event": "ntfy_post",
          "service": "exp.bystate",
          "metric": 2
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "service": "exp.byname",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "metric": 3
        },
        {
          "event": "ntfy_post",
          "metric": 1
        },
        {
          "event": "ingest_response",
          "status": 400
        }
      ]
      """
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "ntfy_post",
            "service": "exp.bystate",
            "metric": 2
          },
          "after": {
            "event": "ntfy_post",
            "service": "exp.byname"
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
            "service": "exp.byname"
          },
          "equals": 1
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "exp.bystate"
          },
          "equals": 2
        },
        {
          "match": {
            "event": "ntfy_post"
          },
          "equals": 3
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "service": "exp.byname"
          },
          "field": "node"
        }
      ]
      """
