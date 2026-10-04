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

    # One shard, because the property under test is that per-key state is held per
    # key. With several shards a wrong implementation holding one remembered state
    # per rule instance would hold several of them by accident, and could pass.
    Given riemannd is configured with:
      """
      {
        "engine.shards": 1
      }
      """
    Given a settle window of 6 seconds
    # The match is by service rather than `true` so that riemann-go's own
    # self-observation events cannot enter this rule and disturb the counts.
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
    # 1. (alpha, cpu.load) ok -> warning. Fires.
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
    # 2. (beta, cpu.load) ok -> warning. Fires: a different identity, whose
    #    remembered state is still `initial`.
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
    # 3. (alpha, cpu.load) warning -> warning. Suppressed. The only one.
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
    # 4. (alpha, cpu.load) warning -> critical. Fires.
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
    # 5. (beta, cpu.load) warning -> critical. Fires.
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
    # 6. (beta, disk.load) ok -> warning. Fires: same host as 5, different
    #    service, so a `by` forking on host alone gets this one wrong later.
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
    # 7. (beta, cpu.load) critical -> warning. Fires. Under a `by` forking on host
    #    alone, event 6 would have left `warning` remembered for beta and this
    #    event would be suppressed.
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
    # The restatement. Catches an implementation that forwards every event.
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
    # Seven events in, six alerts out. This single number is what fails against
    # the two wrong implementations this scenario exists to catch: one remembered
    # state shared across the whole rule gives three, and a `by` forking on host
    # alone gives five.
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
    # Event 7's alert replaced `critical`, so prior_state must be present and
    # non-null. An implementation that reports prior_state only on the first firing
    # for a key fails here.
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
