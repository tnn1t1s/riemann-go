Feature: dryrun-tests-a-rule-that-is-not-installed

  @P15 @holdout
  Scenario: dryrun-tests-a-rule-that-is-not-installed
    A dry run evaluates the rule document in the body, whether or not a rule
    with that id is installed. SPEC.md "HTTP surface (normative)".

    Written from the specification, and derived from what dry run is for rather
    than from any implementation: a preventive control that requires the thing
    it protects against is not one. If a rule must be installed before it can be
    tested, the only way to find out whether it pages someone is to let it.

    It replaces where-refuses-a-non-boolean-predicate, which was burned the same
    day. Expected duration: about 8 seconds.

    Given a settle window of 3 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "seed-index",
          "owner": "arena",
          "partition": "host",
          "match": "true",
          "stream": {
            "op": "where",
            "expr": "true",
            "children": [
              {
                "sink": "index"
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
          "host": "dry-probe",
          "service": "dry.probe",
          "state": "ok",
          "metric": 50,
          "ttl": 120
        }
      ]
      """
    When at 1s the client dry-runs rule "never-installed":
      """
      {
        "id": "never-installed",
        "owner": "arena",
        "partition": "host",
        "match": "service == \"dry.probe\"",
        "stream": {
          "op": "where",
          "expr": "metric > 10",
          "children": [
            {
              "sink": "ntfy"
            }
          ]
        }
      }
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "rule_response",
          "kind": "dryrun",
          "id": "never-installed",
          "status": 200
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "rule": "never-installed"
        },
        {
          "event": "rule_response",
          "kind": "put",
          "id": "never-installed"
        }
      ]
      """
