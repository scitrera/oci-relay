#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
"""Vendor the adapter with exact file hashes; never invent a release/commit pin."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tomllib

def host_updates(host):
    """Plan opt-in bindings before writing anything to the target checkout."""
    updates = {}
    bindings = [
        ('src/sparkrun/core/features.py', r'name\s*=\s*[\'"]plugins\.oci_relay[\'"]',
         '\n\n# Optional OCI Relay binding, installed by scripts/vendor-plugin.py.\n'
         'FEATURE_PLUGIN_OCI_RELAY = register_feature(\n'
         '    FeatureFlag(name="plugins.oci_relay", description="OCI Relay image distribution", default=False)\n)\n'),
        ('src/sparkrun/core/in_tree_plugins.py', r'[\'"]oci_relay[\'"]\s*(?:\]\s*=|:)\s*[\'"]plugins\.oci_relay[\'"]',
         '\n\n# Optional OCI Relay binding, installed by scripts/vendor-plugin.py.\n'
         'IN_TREE_PLUGIN_FEATURES["oci_relay"] = "plugins.oci_relay"\n'),
    ]
    for name, pattern, addition in bindings:
        path = host / name
        text = path.read_text()
        if not re.search(pattern, text):
            if 'oci_relay' in text:
                raise SystemExit('Unrecognized existing OCI Relay binding in ' + name)
            text = text.rstrip() + addition
            ast.parse(text)
            updates[path] = text
    path = host / 'pyproject.toml'
    original = text = path.read_text()
    data = tomllib.loads(text)
    required = ['LICENSE', 'LICENSE_EXCEPTION', 'COPYRIGHT', 'CLA.md', 'README.md', 'VENDORED.toml', 'releases.json']
    package_data = data.get('tool', {}).get('setuptools', {}).get('package-data', {})
    existing = package_data.get('sparkrun.plugins.oci_relay')
    if existing is not None and not set(required).issubset(existing):
        raise SystemExit('Existing OCI Relay package-data needs manual reconciliation')
    if existing is None:
        line = '"sparkrun.plugins.oci_relay" = ' + json.dumps(required) + '\n'
        header = '[tool.setuptools.package-data]'
        if header in text:
            text = text.replace(header, header + '\n' + line, 1)
        else:
            text = text.rstrip() + '\n\n' + header + '\n' + line
    exclude = data.get('tool', {}).get('ruff', {}).get('extend-exclude', [])
    vendor_path = 'src/sparkrun/plugins/oci_relay'
    if vendor_path not in exclude:
        line = 'extend-exclude = ' + json.dumps([*exclude, vendor_path])
        if 'extend-exclude' in data.get('tool', {}).get('ruff', {}):
            pattern = r'(?m)^extend-exclude\s*=\s*\[[^\]]*\]'
            text, count = re.subn(pattern, lambda _: line, text)
            if count != 1:
                raise SystemExit('Unrecognized Ruff exclusion layout')
        elif '[tool.ruff]' in text:
            text = text.replace('[tool.ruff]', '[tool.ruff]\n' + line, 1)
        else:
            text = text.rstrip() + '\n\n[tool.ruff]\n' + line + '\n'
    tomllib.loads(text)
    if text != original:
        updates[path] = text
    return updates


parser = argparse.ArgumentParser()
parser.add_argument("--sparkrun", type=Path, required=True)
parser.add_argument("--development", action="store_true", help="explicitly allow a labeled uncommitted snapshot")
parser.add_argument("--check", action="store_true", help="verify snapshot hashes and required host bindings")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
destination = args.sparkrun.resolve() / "src/sparkrun/plugins/oci_relay"
if args.check:
    if destination.is_symlink():
        raise SystemExit("Vendored directory must not be a symlink")
    lock = tomllib.loads((destination / "VENDORED.toml").read_text())
    if lock.get("schema") != 1 or lock.get("image_distribution_api") != 1:
        raise SystemExit("Unsupported OCI Relay vendor manifest")
    expected = lock["files"]
    actual = {str(p.relative_to(destination)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in destination.rglob("*") if p.is_file()
              and p.name != "VENDORED.toml" and "__pycache__" not in p.parts}
    if any(p.is_symlink() for p in destination.rglob("*")):
        raise SystemExit("Vendored symlinks are forbidden")
    if actual != expected or hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest() != lock["content_sha256"]:
        raise SystemExit("Vendored OCI Relay files differ from their recorded hashes")
    if host_updates(args.sparkrun.resolve()):
        raise SystemExit("Vendored OCI Relay host bindings are incomplete")
    print("Vendored OCI Relay snapshot and host bindings verified")
    raise SystemExit(0)
if not (args.sparkrun / "src/sparkrun/core/image_distribution.py").is_file():
    raise SystemExit("Target needs Sparkrun image-distribution API 1")
source = root / "plugin/src/sparkrun_oci_relay"
files = {str(p.relative_to(source)): p.read_bytes() for p in source.rglob("*")
         if p.is_file() and "__pycache__" not in p.parts and p.suffix in {".py", ".json"}}
for name in ("LICENSE", "LICENSE_EXCEPTION", "COPYRIGHT", "CLA.md", "README.md"):
    files[name] = (root / name).read_bytes()
commit = subprocess.run(["git", "rev-parse", "--verify", "HEAD"], cwd=root, capture_output=True, text=True)
revision = commit.stdout.strip() if commit.returncode == 0 else ""
dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True).strip()
version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
if not args.development:
    release_tag = "v" + version
    ancestry = subprocess.run(["git", "merge-base", "--is-ancestor", release_tag, "HEAD"],
                              cwd=root, capture_output=True)
    engine_changed = subprocess.run(["git", "diff", "--quiet", release_tag, "HEAD", "--", "cmd", "internal", "go.mod", "go.sum"],
                                    cwd=root, capture_output=True)
    releases = json.loads(files["releases.json"])
    if dirty or ancestry.returncode or engine_changed.returncode or not revision:
        raise SystemExit("Production vendoring requires a clean commit containing the matching unchanged engine release; use --development for a worktree")
    if set(releases.get(version, {})) != {"linux/amd64", "linux/arm64"}:
        raise SystemExit("Release checksums must be pinned for both architectures")
hashes = {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())}
content_hash = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
lock = (
    "# Generated by OCI Relay scripts/vendor-plugin.py.\n"
    "schema = 1\n"
    'repository = "https://github.com/scitrera/oci-relay"\n'
    f"version = {json.dumps(version)}\ncommit = {json.dumps(revision)}\n"
    f"development_snapshot = {str(args.development).lower()}\n"
    f"content_sha256 = {json.dumps(content_hash)}\n"
    "image_distribution_api = 1\n\n[files]\n"
)
lock += "".join(json.dumps(name) + " = " + json.dumps(sha) + "\n" for name, sha in hashes.items())
updates = host_updates(args.sparkrun.resolve())
temporary = destination.with_name("oci_relay.staging")
if temporary.exists():
    raise SystemExit("Staging directory already exists; inspect it before retrying")
temporary.mkdir(parents=True)
try:
    for name, data in files.items():
        path = temporary / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    (temporary / "VENDORED.toml").write_text(lock)
    if destination.exists():
        # Refuse to overwrite locally modified vendored source.
        result = subprocess.run([__import__("sys").executable, __file__, "--sparkrun", str(args.sparkrun), "--check"])
        if result.returncode:
            raise SystemExit("Existing snapshot was modified; reconcile before re-vendoring")
        shutil.rmtree(destination)
    temporary.rename(destination)
    for path, text in updates.items():
        path.write_text(text)
finally:
    if temporary.exists():
        shutil.rmtree(temporary)
print("Vendored OCI Relay", version, "development snapshot" if args.development else revision, content_hash)
print("Bundled adapter remains disabled until features.plugins.oci_relay is enabled; disable the installed copy first.")
