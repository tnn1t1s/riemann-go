Feature: rule-lifecycle

  @P14
  Scenario: rule-lifecycle
    A rule is a resource. PUT it and it fires; DELETE it and it stops firing.
    SPEC.md property 14 and "Rule document (normative)". The delete half is the
    half that matters: a rule registry that never removes an instance passes
    every other scenario in this corpus.

    No rules are seeded at the top level here, because the PUT is the property.

    Given a settle window of 5 seconds
    When at 0s the client puts rule "listener-connected":
      """
      {
        "id": "listener-connected",
        "owner": "ops",
        "partition": "host",
        "match": "service == \"ntfy.listen.connected\"",
        "stream": {
          "op": "changed-state",
          "initial": "ok",
          "children": [
            {
              "sink": "ntfy"
            }
          ]
        }
      }
      """
    # Fires while the rule is registered.
    When at 1s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "ntfy.listen.connected",
          "state": "warning",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 3s the client deletes rule "listener-connected"
    # A transition that would alert if the rule were still registered. The distinct
    # state is what makes the silence provable rather than assumed.
    When at 4s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "ntfy.listen.connected",
          "state": "critical",
          "metric": 0,
          "ttl": 300
        }
      ]
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "rule_response",
          "kind": "put",
          "id": "listener-connected",
          "status": 201
        },
        {
          "event": "rule_response",
          "kind": "delete",
          "id": "listener-connected",
          "status": 204
        },
        {
          "event": "ntfy_post",
          "service": "ntfy.listen.connected",
          "state": "warning"
        }
      ]
      """
    # The post-delete transition. Its absence is the delete half.
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "service": "ntfy.listen.connected",
          "state": "critical"
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
            "id": "listener-connected"
          },
          "after": {
            "event": "ntfy_post",
            "service": "ntfy.listen.connected"
          }
        },
        {
          "before": {
            "event": "ntfy_post",
            "service": "ntfy.listen.connected"
          },
          "after": {
            "event": "rule_response",
            "kind": "delete",
            "id": "listener-connected"
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
            "service": "ntfy.listen.connected"
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
            "event": "rule_response",
            "kind": "put",
            "id": "listener-connected"
          },
          "field": "version"
        }
      ]
      """
