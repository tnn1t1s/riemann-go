Feature: where-refuses-a-non-boolean-predicate

  BURNED from the held-out set on 2026-09-30, under scenarios/holdout/README.md
  rule 5. It failed in three of six generations, which is a spec ambiguity
  rather than a bad roll: SEMANTICS.md required the rejection, SPEC.md said
  only that a rule which does not compile is a 400, and the prompt's conflict
  order puts SPEC first. SPEC.md now states that a predicate must be of
  boolean type, so the spec is fitted to this case and it measures nothing
  about generalisation any more.

  @expression_language @http_surface
  Scenario: where-refuses-a-non-boolean-predicate
    Source: upstream `where-test` "return value" and `where*-test`, both of
    which establish that upstream's `where` treats any truthy value as a match:
    `((where 2) :wheeee!)` returns 2 and `(where metric)` passes every event
    carrying a non-nil metric. SEMANTICS.md `where`, first edge case, states the
    riemann-go behavior and the reason: a `where` expression whose type is not
    boolean fails to compile, which is a 400 at `PUT`, and it is not a
    truthiness coercion at evaluation time. "A predicate that is accidentally a
    projection fires on everything, and that is the failure mode hardest to
    notice once a rule is live."

    So this is the same upstream case with the verdict inverted. Upstream
    asserts the coercion happens; riemann-go asserts it is refused before the
    rule can ever run.

    The rejection is asserted at the rule surface rather than at the oracle,
    which HARNESS.md's trace vocabulary allows for rule registration, and the
    consequence is asserted at the oracle: the rejected rule never posts. Both
    halves are needed. A generation that returns 400 and installs the rule
    anyway fails the silence, and one that installs the rule and simply never
    fires it passes the silence for the wrong reason.

    The second rejected rule is the closed-world statement from SPEC.md's
    expression language: an unknown top-level name is a compile error at `PUT`,
    rather than a rule that silently never fires. It shares this scenario
    because it is the same discipline, and it carries its own rule id so a
    failure says which half broke.

    `live-control` is here so the silence is provable. Without it, an
    implementation that dropped every event on the floor would satisfy both
    not_contains clauses.

    Wrong implementations this catch: upstream's truthiness coercion ported
    directly, which posts an extra alert per event from `truthy-where`; a `PUT`
    that accepts any expression that parses and defers the type error to
    evaluation; an expression engine with an open name domain, where
    `bogus_name` reads as null and the rule compiles.

    Every event carries a metric, so the scenario never depends on SEMANTICS.md
    Open question 1, the reading of `metric` on an event that carries none.

    Expected wall clock: about 1.4 s of stimulus plus a 5 s settle.

    Given a settle window of 5 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "live-control",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"pred.probe\"",
          "stream": {
            "op": "where",
            "expr": "metric > 0",
            "children": [
              {
                "sink": "ntfy"
              }
            ]
          }
        }
      ]
      """
    When at 0s the client puts rule "truthy-where":
      """
      {
        "id": "truthy-where",
        "owner": "arena",
        "partition": "host",
        "match": "service == \"pred.probe\"",
        "stream": {
          "op": "where",
          "expr": "metric",
          "children": [
            {
              "sink": "ntfy"
            }
          ]
        }
      }
      """
    When at 0.3s the client puts rule "open-world-where":
      """
      {
        "id": "open-world-where",
        "owner": "arena",
        "partition": "host",
        "match": "service == \"pred.probe\"",
        "stream": {
          "op": "where",
          "expr": "bogus_name == 1",
          "children": [
            {
              "sink": "ntfy"
            }
          ]
        }
      }
      """
    When at 1s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "pred.probe",
          "state": "ok",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 1.4s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "pred.probe",
          "state": "ok",
          "metric": 2,
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
          "id": "truthy-where",
          "status": 400
        },
        {
          "event": "rule_response",
          "kind": "put",
          "id": "open-world-where",
          "status": 400
        },
        {
          "event": "ntfy_post",
          "rule": "live-control",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "rule": "live-control",
          "metric": 2
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "rule": "truthy-where"
        },
        {
          "event": "ntfy_post",
          "rule": "open-world-where"
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
            "id": "truthy-where"
          },
          "after": {
            "event": "ntfy_post",
            "rule": "live-control"
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
            "rule": "live-control"
          },
          "equals": 2
        },
        {
          "match": {
            "event": "ntfy_post"
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
            "rule": "live-control"
          },
          "field": "node"
        }
      ]
      """
