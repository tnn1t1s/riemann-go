Feature: expiry-becomes-event

  @P4 @P5
  Scenario: expiry-becomes-event
    An index entry whose time + ttl has passed produces an event with host and
    service kept and state `expired`, delivered to rules like any other event.
    SPEC.md property 4, carried from src/riemann/core.clj:274-308.

    This is the scenario that makes the index observable through the oracle at
    all. Nothing else in the corpus proves the index exists: without expiry, an
    index that dropped every entry on insert would still pass every other
    scenario here.

    Given a settle window of 8 seconds
    # The index leaf is what makes this identity expire at all. SPEC.md property 5
    # indexes only what a rule routes there, so without it the heartbeat would
    # never enter the index and the TTL would never elapse. It sits before
    # changed-state so the raw event is what gets stored.
    Given these rules are installed:
      """
      [
        {
          "id": "listener-up",
          "owner": "ops",
          "partition": "host",
          "match": "service == \"ntfy.listen.up\"",
          "stream": {
            "op": "where",
            "expr": "true",
            "children": [
              {
                "sink": "index"
              },
              {
                "op": "changed-state",
                "initial": "ok",
                "children": [
                  {
                    "sink": "ntfy"
                  }
                ]
              }
            ]
          }
        }
      ]
      """
    # One beat with a 2 s ttl, then silence. The entry expires during settle.
    When at 0s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "ntfy.listen.up",
          "state": "ok",
          "metric": 1,
          "ttl": 2
        }
      ]
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ntfy_post",
          "host": "ghost",
          "service": "ntfy.listen.up",
          "state": "expired"
        }
      ]
      """
    # The initial `ok` is not a transition away from changed-state's initial value,
    # so it must not alert. Only the expiry does.
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "host": "ghost",
          "service": "ntfy.listen.up",
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
            "state": "expired"
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
            "service": "ntfy.listen.up"
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
            "state": "expired"
          },
          "field": "prior_state"
        }
      ]
      """
