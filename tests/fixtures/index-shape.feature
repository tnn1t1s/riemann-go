Feature: Index shape self-test

  Asserts a property of the harness, not of riemann-go: GET /index is read
  under the one field name SPEC.md's read surface fixes, so a candidate that
  renames it fails instead of having its own name read.

  @P6
  Scenario: the index answers the shape SPEC.md fixes
    Given a settle window of 0.2 seconds
    When at 0s the client queries the index with "service == "fixture""
    Then the recorded trace contains:
      """
      [{"event": "query_response", "kind": "index", "status": 200, "match_count": 1}]
      """
