Feature: expiry-event-carries-only-identity

  @P4 @holdout
  Scenario: expiry-event-carries-only-identity
    Source: SEMANTICS.md "Expiry", "The shape of the synthesized event", which
    states the consequence without a worked example: the synthesized event
    copies only `host` and `service`, so its `metric`, `tags` and `attributes`
    are the event model's defaults and a rule matching a tag together with the
    expired state never fires. Asserts SPEC.md property 4 and that sentence.

    The development corpus's `expiry-becomes-event` asserts only that an expiry
    alert arrives. That passes against an implementation that synthesizes the
    expiry event by copying the whole expiring entry and overwriting `state`,
    which is the obvious implementation and the wrong one. Two rules separate
    them: one matches the expired state alone and must fire, the other matches
    the expired state together with a tag the expiring entry carried and must
    never fire.

    `archive-probe` exists because SEMANTICS.md "Indexing" says there is no
    implicit indexing: an entry reaches the index only through a
    `{"sink":"index"}` leaf. The scenario routes the event there explicitly so
    that it does not depend on whether a generation indexes at ingest.

    Expected wall clock: no stimulus after t=0, a 2 s ttl, and a 9 s settle.

    Given a settle window of 9 seconds
    # archive-probe puts the entry in the index so that it has something to expire
    # from. expiry-any must fire: the expiry event carries host and service, so it
    # matches. expiry-tagged must never fire: the expiring entry carried the tag,
    # the synthesized event does not, and this rule firing means the implementation
    # copied fields the specification says it drops.
    Given these rules are installed:
      """
      [
        {
          "id": "archive-probe",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"agent.tokens.out\"",
          "stream": {
            "sink": "index"
          }
        },
        {
          "id": "expiry-any",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"agent.tokens.out\" && state == \"expired\"",
          "stream": {
            "sink": "ntfy"
          }
        },
        {
          "id": "expiry-tagged",
          "owner": "arena",
          "partition": "host",
          "match": "tagged(\"agent-obs\") && state == \"expired\"",
          "stream": {
            "sink": "ntfy"
          }
        }
      ]
      """
    # One beat with a 2 s ttl, carrying a metric, a tag and an attribute, then
    # silence. The entry expires during the settle window.
    When at 0s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "agent.tokens.out",
          "state": "ok",
          "metric": 1234,
          "ttl": 2,
          "tags": [
            "agent-obs"
          ],
          "attributes": {
            "run_id": "r-42"
          }
        }
      ]
      """
    # Identity survives, metric does not. `metric: null` is the spec's own
    # statement in the alert shape: null when the event carries none.
    Then the recorded trace contains:
      """
      [
        {
          "event": "ntfy_post",
          "rule": "expiry-any",
          "host": "ghost",
          "service": "agent.tokens.out",
          "state": "expired",
          "metric": null
        }
      ]
      """
    # The tag did not survive, so expiry-tagged has nothing to match. The metric
    # did not survive either, which the metric 1234 entry catches: a copy-the-entry
    # expiry fails here even if the tag happened to be dropped. And the live event
    # itself never reaches ntfy, because no rule routes it there.
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "rule": "expiry-tagged"
        },
        {
          "event": "ntfy_post",
          "metric": 1234
        },
        {
          "event": "ntfy_post",
          "state": "ok"
        }
      ]
      """
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "ingest_response",
            "status": 202
          },
          "after": {
            "event": "ntfy_post",
            "rule": "expiry-any"
          }
        }
      ]
      """
    # Exactly one expiry, delivered exactly once. An implementation that
    # re-dispatches the synthesized event through the index leaf and expires it
    # again would show more.
    Then the recorded trace has these counts:
      """
      [
        {
          "match": {
            "event": "ntfy_post",
            "rule": "expiry-any"
          },
          "equals": 1
        },
        {
          "match": {
            "event": "ntfy_post"
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
            "rule": "expiry-any"
          },
          "field": "node"
        }
      ]
      """
