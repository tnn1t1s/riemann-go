Feature: splitp-branch-state-is-per-branch

  @P10 @holdout
  Scenario: splitp-branch-state-is-per-branch
    Source: upstream `splitp-test`'s "Evaluates child streams once at creation
    time" and `split-test`'s "evaluates streams once", translated. Upstream
    counts constructions with an atom: each branch's stream is built once when
    the stream is defined, and four events later the counters still read one. A
    construction count is not observable over HTTP, so the scenario asserts the
    consequence SEMANTICS.md draws from it instead: branch subtrees hold state
    across events rather than being rebuilt per event, and each branch's state
    is independent because branches are separate subtrees rather than separate
    fork keys, so "a `changed-state` inside a branch sees only the events that
    reached that branch and is blind to events that took a sibling".

    A `changed-state` sits in each of the two branches. Three of the four events
    carry state `warning`, and the fourth carries `critical`. What separates the
    implementations is which of them are suppressed:


    metric 15  high branch, first event there, warning against initial   fires

    metric 1   low branch,  first event there, warning against initial   fires

    metric 16  high branch, warning again                            suppressed

    metric 2   low branch,  critical after warning                       fires


    One `changed-state` shared by both branches suppresses metric 1, because the
    event before it left `warning` remembered, and gives two alerts. A subtree
    rebuilt for each event remembers nothing, so metric 16 fires too and it
    gives four. The correct implementation gives three, and the three are
    identifiable individually rather than only countable.

    The branch entry is spelled `threshold` plus `stream`, per SPEC.md's
    combinator table, and `otherwise` holds its subtree directly.

    This is not the fork-key property. There is no `by` here, and SEMANTICS.md
    `splitp` is explicit that branch independence is structural rather than
    keyed. A single identity sends every event, so an implementation that got
    branch independence by accidentally forking on host would not be caught
    here, and is not the subject.

    Wrong implementations this catches: one `changed-state` instance shared
    across the branches of a `splitp`; a branch subtree instantiated per event;
    a `splitp` that delivers to more than one branch, which fires metric 16 from
    the low branch.

    Expected wall clock: about 1.5 s of stimulus plus a 5 s settle.

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
          "id": "branch-state",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"bs.probe\"",
          "stream": {
            "op": "splitp",
            "test": "metric >= {}",
            "branches": [
              {
                "threshold": 10,
                "stream": {
                  "op": "changed-state",
                  "initial": "ok",
                  "children": [
                    {
                      "op": "set",
                      "fields": {
                        "service": "\"bs.high\""
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
            ],
            "otherwise": {
              "op": "changed-state",
              "initial": "ok",
              "children": [
                {
                  "op": "set",
                  "fields": {
                    "service": "\"bs.low\""
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
        }
      ]
      """
    When at 0s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "bs.probe",
          "state": "warning",
          "metric": 15,
          "ttl": 300
        }
      ]
      """
    When at 0.5s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "bs.probe",
          "state": "warning",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 1s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "bs.probe",
          "state": "warning",
          "metric": 16,
          "ttl": 300
        }
      ]
      """
    When at 1.5s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "bs.probe",
          "state": "critical",
          "metric": 2,
          "ttl": 300
        }
      ]
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ntfy_post",
          "service": "bs.high",
          "metric": 15,
          "state": "warning"
        },
        {
          "event": "ntfy_post",
          "service": "bs.low",
          "metric": 1,
          "state": "warning"
        },
        {
          "event": "ntfy_post",
          "service": "bs.low",
          "metric": 2,
          "state": "critical"
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "metric": 16
        },
        {
          "event": "ntfy_post",
          "service": "bs.high",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "service": "bs.high",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "service": "bs.low",
          "metric": 15
        }
      ]
      """
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "ntfy_post",
            "metric": 15
          },
          "after": {
            "event": "ntfy_post",
            "metric": 1
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
            "rule": "branch-state"
          },
          "equals": 3
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "bs.high"
          },
          "equals": 1
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "bs.low"
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
            "metric": 2
          },
          "field": "prior_state"
        }
      ]
      """
