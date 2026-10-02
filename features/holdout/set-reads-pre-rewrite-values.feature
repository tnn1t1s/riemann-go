Feature: set-reads-pre-rewrite-values

  @P11 @holdout
  Scenario: set-reads-pre-rewrite-values
    Source: SEMANTICS.md `set`, first and second edge cases, neither of which
    has a worked example. Every field expression is evaluated against the
    incoming event rather than against the partially rewritten one, so two
    fields that reference each other both read the old values and the key order
    cannot change the result. And the incoming event is not modified, so a
    sibling child of the node above sees the original. Asserts SPEC.md property
    11.

    The rewrite is a swap: `service` takes the old `state` and `state` takes the
    old `service`. A swap is the smallest expression pair that distinguishes the
    correct fold-over-the-original from the obvious sequential implementation,
    and it distinguishes both key orders of that sequential implementation from
    each other as well.

    The `set` node has a sibling ntfy leaf under the same `where`, so the same
    event reaches the oracle twice: once rewritten and once not. An
    implementation that rewrites in place loses the second one.

    Expected wall clock: one event at t=0 plus a 5 s settle.

    Given a settle window of 5 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "swap-fields",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"swap.probe\"",
          "stream": {
            "op": "where",
            "expr": "true",
            "children": [
              {
                "op": "set",
                "fields": {
                  "service": "state",
                  "state": "service"
                },
                "children": [
                  {
                    "sink": "ntfy"
                  }
                ]
              },
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
          "service": "swap.probe",
          "state": "alpha",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ntfy_post",
          "host": "ghost",
          "service": "alpha",
          "state": "swap.probe",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "host": "ghost",
          "service": "swap.probe",
          "state": "alpha",
          "metric": 1
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "service": "alpha",
          "state": "alpha"
        },
        {
          "event": "ntfy_post",
          "service": "swap.probe",
          "state": "swap.probe"
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
            "id": "swap-fields"
          },
          "after": {
            "event": "ntfy_post",
            "rule": "swap-fields"
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
            "rule": "swap-fields"
          },
          "equals": 2
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "alpha"
          },
          "equals": 1
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "swap.probe"
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
            "rule": "swap-fields"
          },
          "field": "node"
        }
      ]
      """
