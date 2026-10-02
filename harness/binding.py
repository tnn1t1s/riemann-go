"""Verify the candidate's binding to its inputs before it is launched.

A riemann-go generation produces several artifacts from one source tree: the
host-native `service` the trial launches and the cross-compiled release
assets. One build manifest binds all of them to the SPEC.md bytes they were
generated from. The trial checks that the artifact it is about to launch is
one of those, and that the expected SPEC.md is the one the manifest names.
The candidate's own claims about itself are never consulted.
"""
import hashlib
import json
from pathlib import Path

CONTRACT = "riemann-go/spec-v1"


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify(command, spec_path, manifest_path):
    if not manifest_path:
        raise ValueError("a build manifest is required to bind the artifact; pass --unbound to waive")
    artifact = Path(command[0])
    if not artifact.is_absolute() or not artifact.is_file():
        raise ValueError("an absolute artifact path is required as command[0]")
    spec = Path(spec_path).read_bytes()
    spec.decode("utf-8")
    manifest_bytes = Path(manifest_path).read_bytes()
    manifest = json.loads(manifest_bytes)
    if manifest.get("contract") != CONTRACT:
        raise ValueError("build manifest mismatch: contract")
    spec_digest = hashlib.sha256(spec).hexdigest()
    if manifest.get("spec_sha256") != spec_digest:
        raise ValueError("build manifest mismatch: spec_sha256")
    artifact_digest = sha256(artifact)
    listed = set()
    if isinstance(manifest.get("artifact_sha256"), str):
        listed.add(manifest["artifact_sha256"])
    artifacts = manifest.get("artifacts")
    if isinstance(artifacts, dict):
        listed.update(v for v in artifacts.values() if isinstance(v, str))
    if artifact_digest not in listed:
        raise ValueError("build manifest mismatch: artifact " + artifact.name + " is not listed")
    return spec, {
        "contract": CONTRACT,
        "spec_sha256": spec_digest,
        "artifact_sha256": artifact_digest,
        "artifact": artifact.name,
        "build_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    }
