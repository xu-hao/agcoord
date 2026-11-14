"""Explicit read-only native validation; no plugin installation or agent calls."""

import json
from pathlib import Path
import shutil
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]


def run(*command):
    print("$ " + " ".join(map(str, command)), flush=True)
    result = subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=60)
    print(result.stdout, flush=True)
    if result.stderr:
        print(result.stderr, flush=True)
    result.check_returncode()
    return result.stdout


run("claude", "plugin", "validate", str(root), "--strict")
checkout = json.loads(run("agy", "plugin", "validate", str(root), "--json"))
assert not any(finding["severity"] == "error" for finding in checkout["findings"])
assert checkout["components"]["skills"] == 1

# Strictly validate the Antigravity payload without contributor files or other host manifests.
with tempfile.TemporaryDirectory(prefix="agcoord-native-plugin-") as directory:
    payload = Path(directory)
    shutil.copytree(root / ".antigravity-plugin", payload / ".antigravity-plugin")
    shutil.copytree(root / "skills", payload / "skills")
    strict = json.loads(run("agy", "plugin", "validate", str(payload), "--strict", "--json"))
    assert not any(finding["severity"] == "error" for finding in strict["findings"])
    assert strict["components"]["skills"] == 1
print("Verified Claude native manifests and Antigravity checkout/exact host payload; no installs")
