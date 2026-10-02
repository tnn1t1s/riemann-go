Feature: where-partitions-with-no-else

  @expression_language @holdout
  Scenario: where-partitions-with-no-else
    Source: upstream `where-test` in riemann/test/riemann/streams_test.clj, the
    "else", "regex" and "tagged-all (multiple tags)" cases. Adjusted, because
    SEMANTICS.md "Downstream" says riemann-go has no else clause and an author
    who wants one writes a second `where` with the negated expression. This
    scenario is that construction, and it asserts the partition is exact: every
    event lands in exactly one of the two siblings.

    Upstream drives services "cat", "dog", nil and "badger" through `(where
    (service #"a") ... (else ...))` and expects cat and badger on one side, dog
    and nil on the other. The nil service has no riemann-go equivalent, since
    `SPEC.md` property 3 rejects an event with no service at ingest, so it is
    replaced by "kitten", taken from the same test's "variable" case, which is a
    service that simply does not match.

    `=~` becomes `matches`, which `SPEC.md`'s expression language states is RE2
    and unanchored. "badger" contains an "a" but does not begin with one, so an
    implementation that anchors the pattern sends it to the wrong sibling.

    The second rule ports the tagged-all case: upstream's `(tagged ["foo"
    "bar"])` requires every named tag, and riemann-go spells that as a
    conjunction of two `tagged` calls, because `SPEC.md` gives `tagged` one name
    per call.

    Wrong implementations this catches: a `where` with an implicit else that
    delivers a non-matching event to its children anyway (the miss count rises
    above two and every event alerts twice); a `where` that passes everything
    downstream (four hits instead of two); an anchored `matches` (badger becomes
    a miss); a `tagged` conjunction that reads as any-of rather than all-of (cat
    and badger alert on the tag rule).

    Expected wall clock: about 1.2 s of stimulus plus a 5 s settle.

    Given a settle window of 5 seconds
    Given these rules are installed:
      """
      [
        {
          "id": "where-split",
          "owner": "arena",
          "partition": "host",
          "match": "tagged(\"wprobe\")",
          "stream": {
            "op": "where",
            "expr": "true",
            "children": [
              {
                "op": "where",
                "expr": "service matches \"a\"",
                "children": [
                  {
                    "op": "set",
                    "fields": {
                      "service": "\"w.hit\""
                    },
                    "children": [
                      {
                        "sink": "ntfy"
                      }
                    ]
                  }
                ]
              },
              {
                "op": "where",
                "expr": "not (service matches \"a\")",
                "children": [
                  {
                    "op": "set",
                    "fields": {
                      "service": "\"w.miss\""
                    },
                    "children": [
                      {
                        "sink": "ntfy"
                      }
                    ]
                  }
                ]
              }
            ]
          }
        },
        {
          "id": "where-all-tags",
          "owner": "arena",
          "partition": "host",
          "match": "tagged(\"wprobe\")",
          "stream": {
            "op": "where",
            "expr": "tagged(\"foo\") and tagged(\"bar\")",
            "children": [
              {
                "op": "set",
                "fields": {
                  "service": "\"w.bothtags\""
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
          "service": "cat",
          "state": "ok",
          "metric": 1,
          "ttl": 300,
          "tags": [
            "wprobe",
            "foo"
          ]
        }
      ]
      """
    When at 0.4s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "dog",
          "state": "ok",
          "metric": 2,
          "ttl": 300,
          "tags": [
            "wprobe",
            "foo",
            "bar"
          ]
        }
      ]
      """
    When at 0.8s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "badger",
          "state": "ok",
          "metric": 3,
          "ttl": 300,
          "tags": [
            "wprobe",
            "bar"
          ]
        }
      ]
      """
    When at 1.2s the emitter posts:
      """
      [
        {
          "host": "ghost",
          "service": "kitten",
          "state": "ok",
          "metric": 4,
          "ttl": 300,
          "tags": [
            "wprobe",
            "foo",
            "bar",
            "baz"
          ]
        }
      ]
      """
    Then the recorded trace contains:
      """
      [
        {
          "event": "ntfy_post",
          "service": "w.hit",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "service": "w.hit",
          "metric": 3
        },
        {
          "event": "ntfy_post",
          "service": "w.miss",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "service": "w.miss",
          "metric": 4
        },
        {
          "event": "ntfy_post",
          "service": "w.bothtags",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "service": "w.bothtags",
          "metric": 4
        }
      ]
      """
    Then the recorded trace excludes:
      """
      [
        {
          "event": "ntfy_post",
          "service": "w.hit",
          "metric": 2
        },
        {
          "event": "ntfy_post",
          "service": "w.hit",
          "metric": 4
        },
        {
          "event": "ntfy_post",
          "service": "w.miss",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "service": "w.miss",
          "metric": 3
        },
        {
          "event": "ntfy_post",
          "service": "w.bothtags",
          "metric": 1
        },
        {
          "event": "ntfy_post",
          "service": "w.bothtags",
          "metric": 3
        },
        {
          "event": "ntfy_post",
          "service": "cat"
        },
        {
          "event": "ntfy_post",
          "service": "dog"
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
            "id": "where-split"
          },
          "after": {
            "event": "ntfy_post",
            "service": "w.hit",
            "metric": 1
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
            "rule": "where-split"
          },
          "equals": 4
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "w.hit"
          },
          "equals": 2
        },
        {
          "match": {
            "event": "ntfy_post",
            "service": "w.miss"
          },
          "equals": 2
        },
        {
          "match": {
            "event": "ntfy_post",
            "rule": "where-all-tags"
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
            "rule": "where-split"
          },
          "field": "node"
        }
      ]
      """
