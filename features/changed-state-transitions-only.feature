Feature: changed-state-transitions-only

  @P8 @P13
  Scenario: changed-state-transitions-only
    changed-state forwards an event only when its state differs from the state
    it last saw for that key. Five events in the same state produce nothing; the
    sixth, in a different state, produces exactly one alert. Ported from
    riemann/test/riemann/streams_test.clj changed-state-test, and the property
    the fleet's ntfy.listen.up rule depends on today.

    Given a settle window of 6 seconds
    Given these rules are installed:
      """
      [
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
      ]
      """
    # Five in state ok. changed-state's initial value is ok, so none is a
    # transition and none may alert.
    When at 0s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "ntfy.listen.connected",
          "state": "ok",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 0.4s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "ntfy.listen.connected",
          "state": "ok",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 0.8s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "ntfy.listen.connected",
          "state": "ok",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 1.2s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "ntfy.listen.connected",
          "state": "ok",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 1.6s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "ntfy.listen.connected",
          "state": "ok",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    # The transition.
    When at 2s the emitter posts:
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
          "event": "ntfy_post",
          "host": "ghost",
          "service": "ntfy.listen.connected",
          "state": "critical"
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "service": "ntfy.listen.connected",
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
            "state": "critical"
          }
        }
      ]
      """
    # The whole property in one assertion: six events in, one alert out.
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
            "event": "ntfy_post",
            "state": "critical"
          },
          "field": "prior_state"
        }
      ]
      """
