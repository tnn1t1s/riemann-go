"""Standard pytest-bdd collection. bin/trial selects the corpus through the environment.

The development corpus is every feature directly under features/. The held-out
set under features/holdout/ is collected only when RIEMANN_GO_HOLDOUT=1, which
bin/trial sets for --holdout; it never enters a development iteration.
"""
import os
from pathlib import Path

from pytest_bdd import scenarios

root = Path(__file__).resolve().parent
selection = Path(os.environ.get("RIEMANN_GO_FEATURES", str(root))).resolve()
paths = sorted(selection.glob("*.feature")) if selection.is_dir() else [selection]
if os.environ.get("RIEMANN_GO_HOLDOUT") == "1" and selection.is_dir():
    paths += sorted((selection / "holdout").glob("*.feature"))
if not paths or any(p.suffix != ".feature" or not p.is_file() for p in paths):
    raise ValueError("no Gherkin feature files selected")
scenarios(*(str(p) for p in paths))
