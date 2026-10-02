Feature: read-surface-allows-a-browser

  @P6 @http_surface
  Scenario: read-surface-allows-a-browser
    The read surface answers a browser, which means every response carries
    Access-Control-Allow-Origin. SPEC.md "HTTP surface (normative)".

    This is here because production found it and the corpus could not. The
    dashboard loads its page from one origin and subscribes to riemann-go on
    another, so without the header every subscription is refused before a byte
    is read: no error in the server, no failed request in its metrics, just
    empty panes. The harness drives the server from Python, where the same-
    origin policy does not exist, so nothing here would ever have noticed.

    Expected duration: about 7 seconds.

    Given a settle window of 3 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "index-everything",
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
    When at 0s the emitter posts:
      """
      [
        {
          "host": "browser-probe",
          "service": "cors.probe",
          "state": "ok",
          "metric": 1,
          "ttl": 120
        }
      ]
      """
    When at 1s the client queries the index with "service == "cors.probe""
    Then the recorded trace contains:
      """
      [
        {
          "event": "query_response",
          "kind": "index",
          "status": 200,
          "match_count": 1
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "query_response",
            "kind": "index"
          },
          "field": "allow_origin"
        }
      ]
      """
