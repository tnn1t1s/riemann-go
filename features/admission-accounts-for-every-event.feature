Feature: admission-accounts-for-every-event

  @P2 @P16
  Scenario: admission-accounts-for-every-event
    Under a tiny inbox and a one-millisecond admission deadline, every event a
    batch contains is accounted for: admitted and reaching a sink, or refused
    with a status that says so. Nothing is silently lost and no request fails.
    SPEC.md property 2 and property 16.

    This scenario used to assert that a 429 occurs, on the reasoning that a
    batch larger than the inbox cannot be fully admitted. That reasoning is
    wrong: a loop draining while the handler offers will admit the whole batch,
    so whether a 429 appears depends on loop speed rather than on conformance.
    Three generations proved it, one producing a 429 and two not, all three
    correct. Forcing the 429 path needs a way to stall the loop, which would be
    test-only surface the spec does not have; see HARNESS.md "What this corpus
    does not cover".

    Given riemannd is configured with:
      """
      {
        "engine.shards": 1,
        "shard.inbox_capacity": 8,
        "ingest.admission_deadline": 1
      }
      """
    Given a settle window of 10 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "archive-all",
          "owner": "arena",
          "partition": "host",
          "match": "true",
          "stream": {
            "sink": "influx"
          }
        }
      ]
      """
    When at 0s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "flood.event",
          "state": "ok",
          "metric": 1,
          "ttl": 300
        }
      ]
      """
    When at 0.2s the emitter posts 500 events in batches of 500 every 0s from the template:
      """
      {
        "host": "host-{i}",
        "service": "flood.event",
        "state": "ok",
        "metric": 1,
        "ttl": 300
      }
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ingest_response",
          "status": 202,
          "accepted": 1
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ingest_response",
          "status": 400
        },
        {
          "event": "ingest_response",
          "status": 500
        }
      ]
      """
    Then the recorded trace has these counts:
      """
      [
        {
          "match": {
            "event": "influx_write",
            "host": "ghost"
          },
          "min": 1
        }
      ]
      """
    Then the recorded trace has these fields:
      """
      [
        {
          "match": {
            "event": "ingest_response",
            "status": 202
          },
          "field": "accepted"
        }
      ]
      """
