# Held-out scenarios

These are not run during development and are not read when editing the spec. They run once, at promotion, to answer a question the development corpus cannot: does an implementation fitted to the scenarios we wrote also satisfy requirements we did not show anyone?

## Why this exists

In this repository the artifact that overfits is the specification, not a model. Every failure in `features/` drives a spec edit, which means the development corpus is simultaneously the thing being optimised against and the thing reporting the score. A generation that is green across `features/` tells you the spec was edited until those cases passed. It tells you nothing about a case nobody wrote down.

That is the ordinary reason for a held-out set, and the ordinary rules apply.

## Rules

1. **Nothing here is read while editing `SPEC.md`, `SEMANTICS.md`, `SCALE.md` or `INVARIANTS.md`.** Not the assertions, not the failure output, not the trace. Reading one to decide a spec edit consumes it.

2. **No document the generator reads names a scenario held out here.** The reading order is `harness/inputs.py`'s `INPUTS` plus `bin/prompt.md`; the generator is told what it must do, never which cases are checked. `HARNESS.md` is not in that order at all and its worked examples name none of these, and `COVERAGE.md` may list them because nothing hands it to the generator either. Stated this way, the next document that wants a coverage table is answered by the rule rather than by a review.

3. **`bin/trial` runs `features/*.feature` only.** The held-out set is collected only with `--holdout`, at promotion, and its result is recorded in the release manifest.

4. **A held-out failure is a finding, not a bug to patch.** It says the spec generalises worse than the development corpus suggested. Recording it is the point.

5. **A specification edit burns the scenario that prompted it.** The trigger is the edit, not the reading. Once a held-out failure has informed a change to `SPEC.md`, `SEMANTICS.md`, `SCALE.md` or `INVARIANTS.md`, that scenario moves permanently into `features/` and a replacement is written here, because it is now part of what the spec was fitted to. A burned scenario never returns.

   Diagnosing a failure is allowed and expected, including reading the generated tree to find the cause. Classifying a failure is the whole purpose of the set, and a failure nobody investigates teaches nothing. What matters is whether the diagnosis ends in a spec edit. A failure that turns out to be a bad roll against a specification that already says the right thing costs nothing: regenerate, and the scenario stays held out.

   The residual risk after a diagnostic read is carried by the reader, not the file. Whoever has read part of an implementation should say so before writing further held-out scenarios that touch the same code path.

6. **Replacements are written from the specification, not from the implementation.** A scenario derived by reading generated Go asserts what that generation happens to do, which is the failure this whole structure exists to avoid.

## Where held-out scenarios come from

Good sources are independent statements of intent rather than restatements of the development corpus:

- The sixty-seven combinator tests in the upstream Clojure suite, which encode a decade of production edge cases and were written with no knowledge of this repository.
- Consequences of `SEMANTICS.md` that it does not spell out as a worked example. A scenario that merely replays a worked example checks transcription, not generalisation.
- Interactions between two combinators, where each is covered alone in `features/` and the pair is covered nowhere.

## Running them

    uv run bin/trial --command '["/absolute/path/to/riemannd"]' --spec <SPEC.md> \
      --build-manifest <build-manifest.json> --holdout -m holdout --out reports/holdout

Promotion to `releases/validated/` requires green on both sets, with both reports kept.
