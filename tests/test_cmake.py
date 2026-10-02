"""Exercise the build graph with a canned agent; never a model call.

The canned agent writes the riemannd stand-in as output/service and a bound
manifest. The conformance trial is registered with CTest but not run here:
the stand-in implements no combinator, so running the corpus against it would
spend two minutes of settle windows to learn nothing about the build graph.
"""
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CMAKE = shutil.which('cmake')


def test_cmake_generation_lifecycle(tmp_path):
    if not CMAKE:
        pytest.skip('CMake is required for build integration testing')
    source = tmp_path / 'source with spaces'
    shutil.copytree(ROOT, source, ignore=shutil.ignore_patterns(
        '.git', '__pycache__', '.pytest_cache', '.venv', 'build', 'build-*',
        'scratch', 'reports', 'traces', 'releases'))
    build = tmp_path / 'build with spaces'
    agent = tmp_path / 'agent.py'
    agent.write_text(f'''import hashlib, json, pathlib, sys
if sys.argv[1] == 'fail': raise SystemExit(9)
prompt = sys.stdin.read()
assert 'SPEC.md' in prompt
out = pathlib.Path('output')
fixture = pathlib.Path({str(ROOT / "tests/riemannd_fixture.py")!r}).read_text()
service = out / 'service'
service.write_text('#!' + sys.executable + '\\n' + fixture)
service.chmod(0o755)
spec = pathlib.Path('SPEC.md').read_bytes()
(out / 'build-manifest.json').write_text(json.dumps({{
    'contract': 'riemann-go/spec-v1',
    'spec_sha256': hashlib.sha256(spec).hexdigest(),
    'artifact_sha256': hashlib.sha256(service.read_bytes()).hexdigest(),
    'artifacts': {{'riemannd-darwin-arm64': hashlib.sha256(service.read_bytes()).hexdigest()}}}}))
''')
    command = tmp_path / 'agent.json'
    command.write_text(json.dumps([sys.executable, str(agent), '{model}']))

    def run(*args, ok=True):
        result = subprocess.run(args, capture_output=True, text=True, timeout=120)
        assert (result.returncode == 0) == ok, result.stdout + result.stderr
        return result

    def configure(model):
        run(CMAKE, '-S', str(source), '-B', str(build),
            '-DRG_MODEL=' + model, '-DRG_AGENT_COMMAND_FILE=' + str(command),
            '-DPython3_EXECUTABLE=' + sys.executable)

    def attempts():
        return list((build / 'generations').glob('gen-*'))

    configure('fixture-one')
    run(CMAKE, '--build', str(build))
    assert len(attempts()) == 1
    assert (build / 'service').exists() and (build / 'build-manifest.json').exists()
    assert (build / 'SPEC.md').read_bytes() == (source / 'SPEC.md').read_bytes()
    attempt = attempts()[0]
    prepared = json.loads((attempt / 'input/manifest.json').read_text())
    assert set(prepared['inputs_sha256']) == {'SPEC.md', 'SEMANTICS.md', 'SCALE.md', 'INVARIANTS.md', 'knowledge/INDEX.md'}
    assert not (attempt / 'input/features').exists() and not (attempt / 'input/harness').exists()
    ctest = str(Path(CMAKE).parent / 'ctest')
    listing = run(ctest, '--test-dir', str(build), '-N')
    assert 'spec-conformance' in listing.stdout
    run(CMAKE, '--build', str(build))
    assert len(attempts()) == 1  # unchanged build spends no model call
    # Observed: GNU Make 3.81 on macOS compares timestamps at whole-second
    # resolution, so an input changed in the same second as the outputs were
    # written is not seen as newer.
    time.sleep(1.1)
    with (source / 'SPEC.md').open('a') as f:
        f.write('\n<!-- build invalidation fixture -->\n')
    run(CMAKE, '--build', str(build))
    assert len(attempts()) == 2
    assert (build / 'SPEC.md').read_bytes() == (source / 'SPEC.md').read_bytes()
    configure('fixture-two')
    run(CMAKE, '--build', str(build))
    assert len(attempts()) == 3
    configure('fail')
    run(CMAKE, '--build', str(build), ok=False)
    assert len(attempts()) == 4
    assert not (build / 'service').exists()
    assert any(json.loads((p / 'invocation.json').read_text())['status'] == 'failed' for p in attempts())
