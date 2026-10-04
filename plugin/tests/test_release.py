# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
import hashlib
import io
import json
import tarfile

import pytest

from sparkrun_oci_relay import release


def elf(path, machine=183):
    header = bytearray(64)
    header[:6] = b"\x7fELF\x02\x01"
    header[18:20] = machine.to_bytes(2, "little")
    path.write_bytes(header)
    return hashlib.sha256(header).hexdigest()


def test_explicit_binary_requires_trusted_hash_and_architecture(tmp_path):
    path = tmp_path / "binary"
    sha = elf(path)
    config = {"binary_paths": {"arm64": str(path)}}
    with pytest.raises(release.BinaryUnavailable, match="trusted"):
        release.acquire("arm64", config, offline=True)
    config["binary_sha256"] = {"arm64": sha}
    assert release.acquire("arm64", config, offline=True) == (path, sha)
    path.write_bytes(b"tampered")
    with pytest.raises(release.BinaryUnavailable, match="checksum"):
        release.acquire("arm64", config, offline=True)
    elf(path, machine=62)
    with pytest.raises(release.BinaryUnavailable, match="arm64"):
        release.verify_elf(path, "arm64")


def test_offline_has_no_network_without_trusted_cache(monkeypatch):
    monkeypatch.setattr(release.urllib.request, "urlopen", lambda *a, **k: pytest.fail("network used"))
    with pytest.raises(release.BinaryUnavailable, match="no trusted release"):
        release.acquire("arm64", {}, offline=True)


@pytest.mark.parametrize("symlink", [False, True])
def test_cached_release_is_verified_and_reextracted(tmp_path, monkeypatch, symlink):
    package = tmp_path / "package"
    package.mkdir()
    monkeypatch.setattr(release, "__file__", str(package / "release.py"))
    cache = tmp_path / "cache" / release.__version__ / "linux-arm64"
    cache.mkdir(parents=True)
    executable = tmp_path / "elf"
    elf(executable)
    with tarfile.open(cache / "release.tar.gz", "w:gz") as archive:
        entry = tarfile.TarInfo("oci-relay")
        if symlink:
            entry.type = tarfile.SYMTYPE
            entry.linkname = "/etc/passwd"
            archive.addfile(entry)
        else:
            entry.size = executable.stat().st_size
            archive.addfile(entry, io.BytesIO(executable.read_bytes()))
    (package / "releases.json").write_text(json.dumps({release.__version__: {"linux/arm64": {
        "url": "https://example.invalid/release.tar.gz", "sha256": release.file_digest(cache / "release.tar.gz")
    }}}))
    (cache / "oci-relay").write_bytes(b"tampered cached executable")
    settings = {"cache_dir": str(tmp_path / "cache")}
    if symlink:
        with pytest.raises(release.BinaryUnavailable, match="regular"):
            release.acquire("arm64", settings, offline=True)
    else:
        binary, sha = release.acquire("arm64", settings, offline=True)
        assert binary.read_bytes() == executable.read_bytes()
        assert sha == release.file_digest(executable)
