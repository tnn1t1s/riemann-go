Feature: changed-state-per-key-independence

  @P7 @P8 @holdout
  Scenario: changed-state-per-key-independence
    Source: upstream `changed-state-test` in
    riemann/test/riemann/streams_test.clj, translated. Asserts SPEC.md property
    7 (`by` partitions state) together with property 8 (`changed-state`
    suppresses repeats), and SEMANTICS.md `by` / `changed-state`.

    Upstream drives seven events across three (host, service) identities through
    one `changed-state` and asserts exactly six reach the child, with the one
    suppressed event being a restatement of a state that identity already held.
    This is that sequence with the states renamed so that the transitions are
    visible at the oracle, and with a distinct `metric` on every event so that
    each firing is identified individually rather than only counted.

    The identities are (alpha, cpu.load), (beta, cpu.load) and (beta,
    disk.load). Event 3 restates (alpha, cpu.load) as `warning`, which that
    identity already holds, so it must be suppressed. Every other event is a
    transition for its own identity even though several of them restate a state
    some *other* identity holds, which is the part a single shared remembered
    state gets wrong.

    Expected wall clock: about 3 s of stimulus plus a 6 s settle.

    Given riemannd is configured with:
      """
      {
        "engine.shards": 1
      }
      """
    Given a settle window of 6 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "state-fanout",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"cpu.load\" || service == \"disk.load\"",
          "stream": {
            "op": "by",
            "fields": [
              "host",
              "service"
            ],
            "children": [
              {
                "op": "changed-state",
                "initial": "ok",
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
          "service": "cpu.load",
          "state": "warning",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 0.4s the emitter posts:
      """
      [
        {
          "host": "beta",
          "service": "cpu.load",
          "state": "warning",
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
          "service": "cpu.load",
          "state": "warning",
          "metric": 3,
          "ttl": 300
        }
      ]
      """
    When at 1.2s the emitter posts:
      """
      [
        {
          "host": "alpha",
          "service": "cpu.load",
          "state": "critical",
          "metric": 4,
          "ttl": 300
        }
      ]
      """
    When at 1.6s the emitter posts:
      """
      [
        {
          "host": "beta",
          "service": "cpu.load",
          "state": "critical",
          "metric": 5,
          "ttl": 300
        }
      ]
      """
    When at 2s the emitter posts:
      """
      [
        {
          "host": "beta",
          "service": "disk.load",
          "state": "warning",
          "metric": 6,
          "ttl": 300
        }
      ]
      """
    When at 2.4s the emitter posts:
      """
      [
        {
          "host": "beta",
          "service": "cpu.load",
          "state": "warning",
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
          "host": "alpha",
          "service": "cpu.load",
          "state": "warning",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "host": "beta",
          "service": "cpu.load",
          "state": "warning",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "host": "alpha",
          "service": "cpu.load",
          "state": "critical",
          "metric": 4
        },
        {
          "event": "ntfy_post",
          "host": "beta",
          "service": "cpu.load",
          "state": "critical",
          "metric": 5
        },
        {
          "event": "ntfy_post",
          "host": "beta",
          "service": "disk.load",
          "state": "warning",
          "metric": 6
        },
        {
          "event": "ntfy_post",
          "host": "beta",
          "service": "cpu.load",
          "state": "warning",
          "metric": 7
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "metric": 3
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
            "id": "state-fanout"
          },
          "after": {
            "event": "ntfy_post",
            "rule": "state-fanout"
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
            "rule": "state-fanout"
          },
          "equals": 6
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "metric": 7
          },
          "field": "prior_state"
        }
      ]
      """
