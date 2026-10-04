Feature: coalesce-emits-expired-identity-once

  @coalesce @holdout
  Scenario: coalesce-emits-expired-identity-once
    Source: upstream `coalesce-test` in riemann/test/riemann/streams_test.clj,
    translated, and SEMANTICS.md `coalesce`, second and fifth edge cases.
    Asserts the convergence property: an identity whose ttl has lapsed appears
    in the emitted set exactly once and is then removed, so a fold over the set
    stops counting a dead identity instead of counting it forever.

    Three identities arrive, one of them with a 2 s ttl. The fold is oversized,
    meaning three or more members, on the arrival that completes the trio and
    again on the first arrival after the short-lived identity has aged out,
    since that arrival is the one that delivers it on its way out. Every later
    arrival sees two members. So the correct count of oversize alerts is exactly
    two: an implementation that never drops an aged-out identity gives four, and
    one that drops it without emitting it gives one.

    Caveat worth stating, because it decides how to read a failure. SPEC.md
    exposes `events` inside a `coalesce` subtree and says nothing about how a
    predicate reads its size, and it does not say which event a sink leaf below
    a `coalesce` receives. This scenario assumes `len(events)` and asserts only
    on the rule id at the oracle, which are the weakest assumptions available. A
    failure at the `PUT` rather than at the sink is a statement about that gap,
    not about the fold.

    Expected wall clock: about 5 s of stimulus plus a 5 s settle.

    # The fold is over the whole stream, so it must run in one place. A `global`
    # rule instantiated per shard would fold each shard's slice separately.
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
          "id": "fold-oversize",
          "owner": "arena",
          "partition": "global",
          "match": "service == \"fold.probe\"",
          "stream": {
            "op": "coalesce",
            "children": [
              {
                "op": "where",
                "expr": "len(events) >= 3",
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
    # alpha has a 2 s ttl and ages out of the fold on its own, without the index
    # reaper's involvement.
    When at 0s the emitter posts:
      """
      [
        {
          "host": "alpha",
          "service": "fold.probe",
          "state": "ok",
          "metric": 1,
          "ttl": 2
        }
      ]
      """
    When at 0.3s the emitter posts:
      """
      [
        {
          "host": "beta",
          "service": "fold.probe",
          "state": "ok",
          "metric": 2,
          "ttl": 300
        }
      ]
      """
    # Completes the trio: the first oversize emission.
    When at 0.6s the emitter posts:
      """
      [
        {
          "host": "gamma",
          "service": "fold.probe",
          "state": "ok",
          "metric": 3,
          "ttl": 300
        }
      ]
      """
    # alpha lapsed at about t=2. This arrival re-evaluates the table, delivers
    # alpha in the set one last time, and retains only the live two: the second and
    # last oversize emission.
    When at 4s the emitter posts:
      """
      [
        {
          "host": "gamma",
          "service": "fold.probe",
          "state": "ok",
          "metric": 4,
          "ttl": 300
        }
      ]
      """
    # From here the fold is two members. Neither of these may alert.
    When at 4.5s the emitter posts:
      """
      [
        {
          "host": "gamma",
          "service": "fold.probe",
          "state": "ok",
          "metric": 5,
          "ttl": 300
        }
      ]
      """
    When at 5s the emitter posts:
      """
      [
        {
          "host": "beta",
          "service": "fold.probe",
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
          "rule": "fold-oversize"
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
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
            "event": "rule_response",
            "kind": "put",
            "id": "fold-oversize"
          },
          "after": {
            "event": "ntfy_post",
            "rule": "fold-oversize"
          }
        }
      ]
      """
    # The convergence property in one number. Four means the dead identity is
    # counted forever; one means it was dropped without its final delivery.
    Then the recorded trace has these counts:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "rule": "fold-oversize"
          },
          "equals": 2
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "rule": "fold-oversize"
          },
          "field": "node"
        }
      ]
      """
