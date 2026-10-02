Feature: Holdout gating self-test

  @holdout
  Scenario: a held-out case is collected only on request
    Given a settle window of 0.2 seconds
    Given these rules are installed:
      """
      [{"id": "alert-all", "owner": "arena", "partition": "host", "match": "true",
        "stream": {"sink": "ntfy"}}]
      """
    When at 0s the emitter posts:
      """
      [{"host": "fixture", "service": "probe", "state": "ok", "metric": 1, "ttl": 300}]
      """
    Then the recorded trace has these counts:
      """
      [{"match": {"event": "ntfy_post"}, "equals": 1}]
      """
