Feature: ddt-skips-the-metricless-event

  @ddt @holdout
  Scenario: ddt-skips-the-metricless-event
    Source: upstream `ddt-immediate-test` in
    riemann/test/riemann/streams_test.clj ("ignore stream without metrics", "do
    nothing the first time"), and SEMANTICS.md `ddt`, first and third edge
    cases. Asserts that an event with no `metric` is ignored entirely, producing
    no output and not becoming the previous event, so the next event carrying a
    metric differentiates across the gap; and that the first event for a fork
    key produces nothing.

    The series is deliberately flat: two readings of 10 with a metric-less event
    between them. A flat series has a rate of exactly zero whatever the elapsed
    time is, which is what makes this assertable on a real clock at all. The
    scenario does not need to know how many milliseconds passed, only that the
    numerator is zero.

    The two `where` nodes under the `ddt` split zero from non-zero and each
    rewrites the service, so the outcome is a service name at the oracle rather
    than a float comparison in the matcher.

    Expected wall clock: about 2 s of stimulus plus a 5 s settle.

    Given a settle window of 5 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "rate-of-change",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"ddt.probe\"",
          "stream": {
            "op": "by",
            "fields": [
              "host",
              "service"
            ],
            "children": [
              {
                "op": "ddt",
                "children": [
                  {
                    "op": "where",
                    "expr": "metric == 0",
                    "children": [
                      {
                        "op": "set",
                        "fields": {
                          "service": "\"ddt.zero\""
                        },
                        "children": [
                          {
                            "sink": "ntfy"
                          }
                        ]
                      }
                    ]
                  },
                  {
                    "op": "where",
                    "expr": "metric != 0",
                    "children": [
                      {
                        "op": "set",
                        "fields": {
                          "service": "\"ddt.nonzero\""
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
          "service": "ddt.probe",
          "state": "ok",
          "metric": 10,
          "ttl": 300
        }
      ]
      """
    When at 1s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "ddt.probe",
          "state": "ok",
          "ttl": 300
        }
      ]
      """
    When at 2s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "ddt.probe",
          "state": "ok",
          "metric": 10,
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
          "service": "ddt.zero",
          "rule": "rate-of-change"
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "service": "ddt.nonzero"
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
            "id": "rate-of-change"
          },
          "after": {
            "event": "ntfy_post",
            "service": "ddt.zero"
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
            "rule": "rate-of-change"
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
            "service": "ddt.zero"
          },
          "field": "node"
        }
      ]
      """
