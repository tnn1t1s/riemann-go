Feature: ingest-accepted

  @P1 @P2
  Scenario: ingest-accepted
    One event posted to /events is admitted and reaches the InfluxDB sink
    carrying its own identity. Asserts the ingest contract of SPEC.md "HTTP
    surface" and backpressure" (202 with an accepted count) and the sink
    contract of SPEC.md "Alert shape (normative)". This is the scenario that
    fails first if nothing works.

    Given a settle window of 5 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "archive-all",
          "owner": "arena",
          "partition": "host",
          "match": "true",
          "stream": {
            "sink": "influx"
          }
        }
      ]
      """
    When at 0s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "agent.tokens.out",
          "state": "ok",
          "metric": 1234,
          "ttl": 300,
          "tags": [
            "agent-obs"
          ]
        }
      ]
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ingest_response",
          "status": 202,
          "accepted": 1
        },
        {
          "event": "influx_write",
          "host": "ghost",
          "measurement": "agent.tokens.out",
          "metric": 1234.0
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ingest_response",
          "status": 429
        },
        {
          "event": "ingest_response",
          "status": 400
        }
      ]
      """
    # A rule must be registered before it can route an event to a sink. The 202 is
    # deliberately not the `before` here: SPEC.md says the server makes no promise
    # that an accepted event reached any sink, so ordering the reply against the
    # write would assert a race rather than a property.
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "rule_response",
            "kind": "put",
            "id": "archive-all"
          },
          "after": {
            "event": "influx_write",
            "host": "ghost"
          }
        }
      ]
      """
    Then the recorded trace has these counts:
      """
      [
        {
          "match": {
            "event": "influx_write",
            "measurement": "agent.tokens.out"
          },
          "min": 1
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "influx_write",
            "measurement": "agent.tokens.out"
          },
          "field": "bucket"
        }
      ]
      """
