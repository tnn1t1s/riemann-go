Feature: splitp-takes-the-first-branch-only

  @P10 @P13 @holdout
  Scenario: splitp-takes-the-first-branch-only
    Source: upstream `split-test`, `split*-test` and `splitp-test` in
    riemann/test/riemann/streams_test.clj, which are the same dispatch written
    three ways. All three drive metrics 15, 8 and 2 through two thresholds and a
    default, and all three expect exactly one labelled event per input: 15
    critical, 8 warning, 2 ok. This is that case in riemann-go's `splitp`.

    Upstream writes the comparison into the form, as `(> metric 10)` or `(splitp
    <= metric 10 ...)`. SPEC.md instead puts a `{}` placeholder in the node's
    `test` and substitutes each branch's `threshold`, so the comparison and its
    direction live in one string the rule author writes. `metric >= {}` with
    thresholds 10 and 5 is upstream's `splitp <=` reversed, which is the same
    predicate.

    A branch entry is an object with exactly two keys, `threshold` and `stream`,
    and `otherwise` holds a subtree directly, per SPEC.md's combinator table.
    The `stream` key contributes no segment of its own to the node path, so the
    sink inside the first branch is entry 0 of that branch's `set` node children
    and its path is `stream/branches/0/0`.

    Dropped from the port: the "Without a default" halves. Upstream's `splitp`
    throws and its `split*` silently drops in exactly the same situation, and
    SEMANTICS.md Open question 3 records that riemann-go has not chosen between
    them. A scenario asserting either answer would be asserting a decision
    nobody has made.

    The node path is asserted by exact value on two of the three posts rather
    than by presence. SPEC.md's "Node paths" section makes the path mechanical
    precisely so a scenario can do this, and no scenario in this directory does.
    Every other held-out scenario only requires the field to exist, which passes
    against a path built by any rule at all.

    Wrong implementations this catches: branches evaluated in threshold order
    rather than array order, or evaluated all at once and the loosest taken,
    either of which sends metric 15 to the warning branch; a `splitp` that
    delivers the event to every branch whose test holds, which gives five posts
    instead of three; an `otherwise` that is reached even when a branch matched,
    which gives four; a node path that numbers a branch by its threshold or
    omits the `branches` segment.

    Expected wall clock: about 1 s of stimulus plus a 5 s settle.

    Given a settle window of 5 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "dispatch",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"split.probe\"",
          "stream": {
            "op": "splitp",
            "test": "metric >= {}",
            "branches": [
              {
                "threshold": 10,
                "stream": {
                  "op": "set",
                  "fields": {
                    "state": "\"critical\""
                  },
                  "children": [
                    {
                      "sink": "ntfy"
                    }
                  ]
                }
              },
              {
                "threshold": 5,
                "stream": {
                  "op": "set",
                  "fields": {
                    "state": "\"warning\""
                  },
                  "children": [
                    {
                      "sink": "ntfy"
                    }
                  ]
                }
              }
            ],
            "otherwise": {
              "op": "set",
              "fields": {
                "state": "\"ok\""
              },
              "children": [
                {
                  "sink": "ntfy"
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
          "service": "split.probe",
          "state": "raw",
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
          "service": "split.probe",
          "state": "raw",
          "metric": 8,
          "ttl": 300
        }
      ]
      """
    When at 1s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "split.probe",
          "state": "raw",
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
          "metric": 15,
          "state": "critical"
        },
        {
          "event": "ntfy_post",
          "metric": 8,
          "state": "warning"
        },
        {
          "event": "ntfy_post",
          "metric": 2,
          "state": "ok"
        },
        {
          "event": "ntfy_post",
          "metric": 15,
          "node": "stream/branches/0/0"
        },
        {
          "event": "ntfy_post",
          "metric": 2,
          "node": "stream/otherwise/0"
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "metric": 15,
          "state": "warning"
        },
        {
          "event": "ntfy_post",
          "metric": 15,
          "state": "ok"
        },
        {
          "event": "ntfy_post",
          "metric": 8,
          "state": "critical"
        },
        {
          "event": "ntfy_post",
          "metric": 8,
          "state": "ok"
        },
        {
          "event": "ntfy_post",
          "metric": 2,
          "state": "warning"
        },
        {
          "event": "ntfy_post",
          "metric": 2,
          "state": "critical"
        },
        {
          "event": "ntfy_post",
          "state": "raw"
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
            "rule": "dispatch"
          },
          "equals": 3
        },
        {
          "match": {
            "event": "ntfy_post",
            "state": "critical"
          },
          "equals": 1
        },
        {
          "match": {
            "event": "ntfy_post",
            "state": "warning"
          },
          "equals": 1
        },
        {
          "match": {
            "event": "ntfy_post",
            "state": "ok"
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
            "metric": 8
          },
          "field": "node"
        }
      ]
      """
