# What the riemann-surface cutover needs

Evidence, not product code. `shim.py` is a throwaway translator that let the
existing dashboard render live fleet state out of riemann-go, which is how the
list below was established rather than guessed.

The dashboard already speaks SSE, so nothing structural is required. Four
things differ, and each was found by watching a pane fail:

1. **Endpoint.** The dashboard opens `/index?query=<q>`; riemann-go serves
   `/subscribe?q=<expr>`. It also wants unnamed data frames, where riemann-go
   names the snapshot frames.
2. **Timestamps.** The dashboard calls `Date.parse` on `time`, so it needs an
   ISO-8601 string. riemann-go sends float seconds.
3. **Query grammar.** Saved queries are in the retired grammar: `=` for
   equality, `and`, bare `tagged "x"`, and `=~` for SQL-style wildcards, which
   has no expr equivalent and becomes a `matches` regex.
4. **Backslashes in a regex.** A regex inside an expr string literal has its
   backslashes doubled, because the literal consumes one level. `SEMANTICS.md`'s
   syntax table shows this; a translator that emits single ones gets a 400.

Run it with the dashboard pointed at the shim:

    python3 probes/dash-cutover/shim.py                  # 5561 -> riemannd on 5560
    riemann-surface --listen 127.0.0.1:4570 --config probes/dash-cutover/surface-config.json

`fleet-rules.json` is the fleet's five production rules in the rule format, used
for the same run.

The real cutover is four small changes in the dashboard's own subscription
code, at which point the shim is deleted. Until then this records what those
changes are.
