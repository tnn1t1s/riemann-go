"""What riemann-go hands to the generator: its contract, pinned inputs and prompt.

The input list is the reading order fixed in bin/prompt.md. Nothing from
features/, harness/, tests/ or releases/ is ever listed here.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = "riemann-go/spec-v1"
INPUTS = ["SPEC.md", "SEMANTICS.md", "SCALE.md", "INVARIANTS.md", "knowledge/INDEX.md"]
PROMPT = ROOT / "bin/prompt.md"


def arguments():
    args = ["--contract", CONTRACT, "--root", str(ROOT), "--prompt", str(PROMPT), "--spec", "SPEC.md"]
    for name in INPUTS:
        args += ["--input", name]
    return args
