"""Standard pytest-bdd collection. bin/trial selects the corpus through the environment.

The development corpus is every feature in the selected directory. The
held-out set under its holdout/ subdirectory is collected only when
RIEMANN_GO_HOLDOUT=1, which bin/trial sets for --holdout; it never enters a
development iteration.
"""
import os
from pathlib import Path

from pytest_bdd import scenarios

from riemann_harness.features import ENV, select

root = Path(__file__).resolve().parent
paths = select(root)
selection = Path(os.environ.get(ENV, str(root))).resolve()
if os.environ.get("RIEMANN_GO_HOLDOUT") == "1" and selection.is_dir():
    paths += sorted(str(p) for p in (selection / "holdout").glob("*.feature"))
scenarios(*paths)
