"""The trial launches only an artifact its manifest binds to the expected SPEC.md."""
import hashlib
import json

import pytest

from harness.binding import CONTRACT, verify


def make(tmp_path, spec=b"# spec\n", listed="artifact_sha256", contract=CONTRACT):
    artifact = tmp_path / "service"
    artifact.write_bytes(b"#!/bin/sh\nexit 0\n")
    artifact.chmod(0o755)
    spec_path = tmp_path / "SPEC.md"
    spec_path.write_bytes(spec)
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    manifest = {"contract": contract, "spec_sha256": hashlib.sha256(spec).hexdigest()}
    if listed == "artifact_sha256":
        manifest["artifact_sha256"] = digest
    elif listed == "artifacts":
        manifest["artifacts"] = {"riemannd-darwin-arm64": digest, "riemannd-linux-amd64": "0" * 64}
    elif listed == "other":
        manifest["artifacts"] = {"riemannd-linux-amd64": "0" * 64}
    manifest_path = tmp_path / "build-manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return artifact, spec_path, manifest_path


def test_native_and_release_artifacts_bind(tmp_path):
    for listed in ("artifact_sha256", "artifacts"):
        artifact, spec, manifest = make(tmp_path, listed=listed)
        expected, binding = verify([str(artifact)], spec, manifest)
        assert expected == b"# spec\n"
        assert binding["contract"] == CONTRACT and binding["artifact"] == "service"


@pytest.mark.parametrize("mismatch", ["artifact", "spec", "missing", "contract", "unlisted", "relative"])
def test_mismatches_are_rejected(tmp_path, mismatch):
    artifact, spec, manifest = make(tmp_path,
                                    listed="other" if mismatch == "unlisted" else "artifact_sha256",
                                    contract="riemann-graph/spec-v1" if mismatch == "contract" else CONTRACT)
    if mismatch == "artifact":
        artifact.write_bytes(artifact.read_bytes() + b"# changed\n")
    if mismatch == "spec":
        spec.write_bytes(spec.read_bytes() + b"changed\n")
    command = ["service"] if mismatch == "relative" else [str(artifact)]
    with pytest.raises(ValueError):
        verify(command, spec, None if mismatch == "missing" else manifest)
