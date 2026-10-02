Feature: provenance-on-alert

  @P10 @P11 @P13
  Scenario: provenance-on-alert
    Every ntfy post names the rule that produced it, that rule's version, its
    owner, the prior state changed-state left, and the node path traversed, so
    an alert consumer can act without opening the config. SPEC.md "Alert shape
    (normative)". The existing Clojure alert format carries none of these, which
    is why this is a scenario and not an assumption.

    Presence, not value: the version is dynamic and the node path depends on the
    tree, so field_exists is the right operator. The one value asserted is
    `rule`, because a post that names the wrong rule is worse than one that
    names none.

    Given a settle window of 6 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "atlas-cost",
          "owner": "atlas",
          "version": 1,
          "partition": "host",
          "match": "tagged(\"agent-obs\") && service == \"agent.cost\"",
          "stream": {
            "op": "splitp",
            "test": "{} < metric",
            "branches": [
              {
                "threshold": 20.0,
                "stream": {
                  "op": "set",
                  "fields": {
                    "state": "\"critical\""
                  },
                  "children": [
                    {
                      "sink": "index"
                    },
                    {
                      "ref": "transition"
                    }
                  ]
                }
              },
              {
                "threshold": 5.0,
                "stream": {
                  "op": "set",
                  "fields": {
                    "state": "\"warning\""
                  },
                  "children": [
                    {
                      "sink": "index"
                    },
                    {
                      "ref": "transition"
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
                  "sink": "index"
                },
                {
                  "ref": "transition"
                }
              ]
            }
          },
          "bindings": {
            "transition": {
              "op": "changed-state",
              "initial": "ok",
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
          "service": "agent.cost",
          "metric": 1.2,
          "ttl": 300,
          "tags": [
            "agent-obs"
          ]
        }
      ]
      """
    When at 1s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "agent.cost",
          "metric": 7.5,
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
          "event": "ntfy_post",
          "host": "ghost",
          "service": "agent.cost",
          "state": "warning",
          "rule": "atlas-cost"
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "service": "agent.cost",
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
            "id": "atlas-cost"
          },
          "after": {
            "event": "ntfy_post",
            "rule": "atlas-cost"
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
            "rule": "atlas-cost"
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
            "rule": "atlas-cost"
          },
          "field": "version"
        },
        {
          "match": {
            "event": "ntfy_post",
            "rule": "atlas-cost"
          },
          "field": "owner"
        },
        {
          "match": {
            "event": "ntfy_post",
            "rule": "atlas-cost"
          },
          "field": "prior_state"
        },
        {
          "match": {
            "event": "ntfy_post",
            "rule": "atlas-cost"
          },
          "field": "node"
        },
        {
          "match": {
            "event": "ntfy_post",
            "rule": "atlas-cost"
          },
          "field": "metric"
        }
      ]
      """
