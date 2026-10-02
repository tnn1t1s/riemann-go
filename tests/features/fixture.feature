Feature: Harness self-test over the canned riemannd stand-in

  Asserts a property of the harness, not of riemann-go: the step table, the
  session lifecycle, the receiver and the matcher compose into a verdict that
  follows what arrived at the sink receiver.

  @P1
  Scenario: one event routed to ntfy arrives with provenance
    Given a settle window of 0.2 seconds
    Given these rules are installed:
      """
      [
        {"id": "alert-all", "owner": "arena", "partition": "host", "match": "true",
         "stream": {"sink": "ntfy"}}
      ]
      """
    When at 0s the emitter posts:
      """
      [{"host": "fixture", "service": "probe", "state": "ok", "metric": 1, "ttl": 300}]
      """
    Then the recorded trace contains:
      """
      [{"event": "ingest_response", "status": 202, "accepted": 1},
       {"event": "ntfy_post", "rule": "alert-all", "host": "fixture", "service": "probe", "state": "ok"}]
      """
    Then the recorded trace excludes:
      """
      [{"event": "ntfy_post", "state": "wrong"}]
      """
    Then the recorded trace has this order:
      """
      [{"before": {"event": "rule_response", "kind": "put", "id": "alert-all"},
        "after": {"event": "ntfy_post", "rule": "alert-all"}}]
      """
    Then the recorded trace has these counts:
      """
      [{"match": {"event": "ntfy_post"}, "equals": 1}]
      """
    Then the recorded trace has these fields:
      """
      [{"match": {"event": "ntfy_post", "rule": "alert-all"}, "field": "version"}]
      """
