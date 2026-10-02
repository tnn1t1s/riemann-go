Feature: set-derives-fields-from-the-incoming-event

  @P11 @holdout
  Scenario: set-derives-fields-from-the-incoming-event
    Source: three upstream tests of the same shape in
    riemann/test/riemann/streams_test.clj, all translated into riemann-go's
    `set`, which SEMANTICS.md describes as upstream's `with` with expressions in
    place of literal values.

    `with-map` sets a field to a constant and carries every other field through:
    `(with {:service "foo"} ...)` on `{:service "bar" :test "baz"}` yields
    service "foo" with "baz" untouched.

    `adjust-test` derives a field from the event. Its first case is `(adjust
    [:state str " 2"])`, which appends to the event's own state, and it drives
    an empty event through to get `" 2"` from a state that is not there. Its
    second case is `#(assoc % :metric (count (:tags %)))`, a metric computed
    from the tag list.

    `smap-test`'s "increment" case is `(smap inc)` over metrics, which is the
    same operation once `smap`'s general function is restricted to one named
    field.

    Two of these translate with a caveat. Upstream's `with` deletes a key when
    the value is nil (src/riemann/streams.clj:1364-1366), and SEMANTICS.md Open
    question 4 records that riemann-go has not decided what a `set` expression
    evaluating to null does, so nothing here evaluates to null. `smap-test`'s
    other case, "ignores nil values", and the whole of `smap*-test`, are about a
    stream that emits nil, which riemann-go's event model has no equivalent for.

    The second event is the empty-event row of `adjust-test`: it carries no
    state and no tags, so the expressions must read SPEC.md's documented
    defaults, an empty string and an empty array, and produce "-adj" and 0. An
    implementation that skips an expression whose referenced field is absent, or
    that carries the old value through, fails on that event alone.

    Wrong implementations this catches: a `set` that assigns the expression
    source text rather than its value; a `set` that drops fields it does not
    name, which loses the host and fails every `contains` here; an absent `tags`
    read as null rather than as an empty array, which makes `len(tags)` an error
    or a nil instead of 0; an absent `state` treated as missing rather than as
    "", which yields no alert or the wrong string.

    Expected wall clock: about 0.5 s of stimulus plus a 5 s settle.

    Given a settle window of 5 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "derive",
          "owner": "arena",
          "partition": "host",
          "match": "service == \"set.probe\"",
          "stream": {
            "op": "where",
            "expr": "true",
            "children": [
              {
                "op": "set",
                "fields": {
                  "service": "\"set.derived\"",
                  "metric": "metric + 1",
                  "state": "state + \"-adj\""
                },
                "children": [
                  {
                    "sink": "ntfy"
                  }
                ]
              },
              {
                "op": "set",
                "fields": {
                  "service": "\"set.tagcount\"",
                  "metric": "len(tags)"
                },
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
    When at 0s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "set.probe",
          "state": "ok",
          "metric": 10,
          "ttl": 300,
          "tags": [
            "foo",
            "bar"
          ],
          "description": "carried"
        }
      ]
      """
    When at 0.5s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "set.probe",
          "metric": 5,
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
          "service": "set.derived",
          "state": "ok-adj",
          "metric": 11
        },
        {
          "event": "ntfy_post",
          "host": "ghost",
          "service": "set.tagcount",
          "state": "ok",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "service": "set.derived",
          "state": "-adj",
          "metric": 6
        },
        {
          "event": "ntfy_post",
          "service": "set.tagcount",
          "metric": 0
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "service": "set.probe"
        },
        {
          "event": "ntfy_post",
          "service": "set.derived",
          "metric": 10
        },
        {
          "event": "ntfy_post",
          "service": "set.derived",
          "metric": 5
        },
        {
          "event": "ntfy_post",
          "service": "set.tagcount",
          "state": "ok-adj"
        },
        {
          "event": "ingest_response",
          "status": 400
        }
      ]
      """
    Then the recorded trace has this order:
      """
      [
        {
          "before": {
            "event": "ntfy_post",
            "service": "set.derived",
            "metric": 11
          },
          "after": {
            "event": "ntfy_post",
            "service": "set.derived",
            "metric": 6
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
            "rule": "derive"
          },
          "equals": 4
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "set.derived"
          },
          "equals": 2
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "set.tagcount"
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
            "rule": "derive"
          },
          "field": "node"
        }
      ]
      """
