# A predicate that was a projection

Burned from the held-out set on 2026-09-30 after failing in three of six
generations. Now `scenarios/where-refuses-a-non-boolean-predicate.yaml`.

## What the ambiguity was

`SEMANTICS.md` said a `where` whose expression is not boolean fails to compile
and is a `400` at `PUT`. `SPEC.md` said only that a rule which does not
compile is a `400`, without saying that a predicate must be boolean. The
generator's conflict order puts `SPEC.md` first, so a generation that compiled
expressions without requiring a boolean type was following the normative
document and rejecting nothing.

Three of six generations did exactly that. That is not a bad roll; it is a
document that does not say what it means.

## Why it matters

Upstream's `where` returns the expression's value and treats anything truthy
as a match, so `(where metric)` matches every event that has a metric. A
predicate that is accidentally a projection fires on everything, and it does
so quietly: the rule works, the alerts arrive, and nobody looks again until
the volume is wrong.

## What changed

`SPEC.md`'s rule surface now states that an expression used as a predicate, in
a rule's `match`, a `where`'s `expr`, or a `splitp` `test`, must be of boolean
type, and that there is no truthiness coercion at evaluation time.

## Replacement

`scenarios/holdout/dryrun-tests-a-rule-that-is-not-installed.yaml`, written
from the specification rather than from any implementation.
