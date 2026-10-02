Feature: by-tuple-fork-delivers-every-event

  @P7 @P8 @holdout
  Scenario: by-tuple-fork-delivers-every-event
    Source: upstream `by-multiple` in riemann/test/riemann/streams_test.clj,
    translated. Upstream drives eight events over three (host, service) tuples
    through `(by [:host :service] ...)`, has each branch assert that every event
    it receives carries the tuple that branch was created for, and then asserts
    that the total delivered equals the number of events sent. This is that
    sequence with upstream's `[[1 :a] [1 :b] [1 :a] [2 :a] [2 :a] [1 :a] [2 :a]
    [1 :b]]` preserved exactly, hosts and services renamed, and a distinct
    metric per event so the oracle identifies each one individually.

    Branch purity is not directly observable at a sink, because an event carries
    its own host and service whichever fork it met. What is observable is the
    two halves upstream's assertions decompose into, and the scenario asserts
    both:

    Transparency. The first child of the `by` is a sink leaf, so all eight
    events must arrive with their own identity. SEMANTICS.md `by`, third edge
    case: "The first event for a key creates the fork and is then delivered to
    it, so the first event is not lost. Creation and delivery are one step."

    The tuple is the whole tuple. The second child is a `changed-state`, and all
    eight events carry the same state, so exactly one event per fork fires: the
    first of each. Three tuples, three alerts, and they are metrics 1, 2 and 4.
    Under a `by` forking on host alone there are two forks and two alerts; under
    a `by` that ignores its fields there is one.

    `changed-state-per-key-independence` in this directory also distinguishes
    tuple forking from host forking, but through a sequence of state
    transitions. This one holds the state constant and varies only the key,
    which is what upstream's test does, and it is the only scenario here that
    asserts the fork-creating event is delivered rather than consumed.

    Wrong implementations this catches: a `by` that creates the fork and drops
    the event that created it, losing metrics 1, 2 and 4 from the pass-through
    and firing nothing at all downstream of the `changed-state`; a `by` that
    delivers an event to every existing fork, which raises both counts; a fork
    key built from the first field alone or from no field at all.

    Expected wall clock: about 2.8 s of stimulus plus a 5 s settle.

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
          "id": "tuple-fork",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"by.a\" || service == \"by.b\"",
          "stream": {
            "op": "by",
            "fields": [
              "host",
              "service"
            ],
            "children": [
              {
                "sink": "ntfy"
              },
              {
                "op": "changed-state",
                "initial": "ok",
                "children": [
                  {
                    "op": "set",
                    "fields": {
                      "service": "\"by.first\""
                    },
                    "children": [
                      {
                        "sink": "ntfy"
                      }
                    ]
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
          "host": "h1",
          "service": "by.a",
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
          "host": "h1",
          "service": "by.b",
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
          "host": "h1",
          "service": "by.a",
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
          "host": "h2",
          "service": "by.a",
          "state": "warning",
          "metric": 4,
          "ttl": 300
        }
      ]
      """
    When at 1.6s the emitter posts:
      """
      [
        {
          "host": "h2",
          "service": "by.a",
          "state": "warning",
          "metric": 5,
          "ttl": 300
        }
      ]
      """
    When at 2s the emitter posts:
      """
      [
        {
          "host": "h1",
          "service": "by.a",
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
          "host": "h2",
          "service": "by.a",
          "state": "warning",
          "metric": 7,
          "ttl": 300
        }
      ]
      """
    When at 2.8s the emitter posts:
      """
      [
        {
          "host": "h1",
          "service": "by.b",
          "state": "warning",
          "metric": 8,
          "ttl": 300
        }
      ]
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ntfy_post",
          "service": "by.a",
          "host": "h1",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "service": "by.b",
          "host": "h1",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "service": "by.a",
          "host": "h2",
          "metric": 4
        },
        {
          "event": "ntfy_post",
          "service": "by.a",
          "host": "h1",
          "metric": 3
        },
        {
          "event": "ntfy_post",
          "service": "by.a",
          "host": "h2",
          "metric": 5
        },
        {
          "event": "ntfy_post",
          "service": "by.a",
          "host": "h1",
          "metric": 6
        },
        {
          "event": "ntfy_post",
          "service": "by.a",
          "host": "h2",
          "metric": 7
        },
        {
          "event": "ntfy_post",
          "service": "by.b",
          "host": "h1",
          "metric": 8
        },
        {
          "event": "ntfy_post",
          "service": "by.first",
          "host": "h1",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "service": "by.first",
          "host": "h1",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "service": "by.first",
          "host": "h2",
          "metric": 4
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "service": "by.first",
          "metric": 3
        },
        {
          "event": "ntfy_post",
          "service": "by.first",
          "metric": 5
        },
        {
          "event": "ntfy_post",
          "service": "by.first",
          "metric": 8
        }
      ]
      """
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "ntfy_post",
            "service": "by.first",
            "metric": 1
          },
          "after": {
            "event": "ntfy_post",
            "service": "by.first",
            "metric": 4
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
            "service": "by.a"
          },
          "equals": 6
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "by.b"
          },
          "equals": 2
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "by.first"
          },
          "equals": 3
        },
        {
          "match": {
            "event": "ntfy_post",
            "rule": "tuple-fork"
          },
          "equals": 11
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "service": "by.first"
          },
          "field": "prior_state"
        }
      ]
      """
