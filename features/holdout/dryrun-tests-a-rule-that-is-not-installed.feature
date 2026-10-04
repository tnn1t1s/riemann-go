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
    # Give the ring something for a dry run to replay.
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
    # A rule this server has never seen. The id is deliberately absent from the
    # rule set: an implementation that looks the id up first answers 404 here.
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
    # The dry run answered, rather than refusing an id it does not hold.
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
    # And it touched nothing live: a dry run that actually alerts is worse than one
    # that refuses. The put entry is here because the rule must not have been
    # installed as a side effect of testing it.
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
