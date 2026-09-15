# harness/

Drives a riemann-go binary, records what reached an external sink receiver,
and evaluates a scenario's expectations against that record.

`../HARNESS.md` is the authoritative spec. The principle:

> If adding a riemann-go behavior requires editing Python in the harness,
> the harness is too smart.

## Layout

- `sinks.py` — the oracle. An HTTP server on 127.0.0.1 accepting ntfy publishes
  and InfluxDB v2 line-protocol writes, recording every request verbatim, with
  per-sink injected latency and failure set by method call rather than by an
  endpoint the code under test could reach.
- `observer.py` — turns those records plus harness-emitted events into the
  trace vocabulary. `ntfy_provenance` is the one function that knows where a
  rule id and a prior state sit in an alert body.
- `matcher.py` — cue's, code byte-identical. Five operators, no domain concepts.
- `score.py` — categories. The matcher's verdict decides the pass.
- `run.py` — lifecycle, stimulus, report, and the `--dry` self-test.
- `adapters/riemannd.py` — the only file naming riemann-go's surface.

## Running

```
python3 -m venv .venv
.venv/bin/pip install -r harness/requirements.txt

# Self-test. Needs no binary and no network beyond loopback.
.venv/bin/python -m harness.run --dry

# One scenario against a build.
.venv/bin/python -m harness.run \
  --scenario scenarios/expiry-becomes-event.yaml \
  --binary bin/riemannd \
  --report-out reports/expiry-becomes-event.json \
  --trace-out  traces/expiry-becomes-event.jsonl \
  --log-out    logs/expiry-becomes-event.log
```

`--log-out` gets riemannd's whole log, not the report's 40-line tail, because
the diagnostician reads it. The file is written even when empty, so an absent
one means the run did not get that far rather than that riemannd was quiet.
`score.category` in the report is one of `compile_error`, `start_error`,
`ingest_error`, `rule_error`, `sink_error`, `observer_error`,
`predicate_violation` or `GREEN`.

Exit status is 0 when the score is 1.0 and 1 otherwise. The harness picks
ephemeral ports for both riemannd and the sink receiver, so runs do not collide,
and it brings up and tears down everything it starts.

## The adapter

Flag names in `adapters/riemannd.py` are this adapter's proposal until SPEC.md's
CLI section is normative. It is the single place to change them. A generation
whose surface the adapter cannot drive is a spec violation, not an adapter gap;
do not add a second adapter.

Adapter methods are `start`, `stop`, `ready`, `post_events`, `put_rule`,
`delete_rule` and `query_index`. A method named after a combinator, a threshold
or a state would be a defect.
