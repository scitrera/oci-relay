# SPDX-FileCopyrightText: 2026 Spark Arena
# SPDX-License-Identifier: Apache-2.0
import hashlib
import http.client
import io
import json
import ssl
import tarfile
import urllib.error

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
    with pytest.raises(release.BinaryInvalid, match="trusted"):
        release.acquire("arm64", config, offline=True)
    config["binary_sha256"] = {"arm64": sha}
    assert release.acquire("arm64", config, offline=True) == (path, sha)
    path.write_bytes(b"tampered")
    with pytest.raises(release.BinaryInvalid, match="checksum"):
        release.acquire("arm64", config, offline=True)
    elf(path, machine=62)
    with pytest.raises(release.BinaryInvalid, match="arm64"):
        release.verify_elf(path, "arm64")


@pytest.mark.parametrize("pinned", [False, True])
def test_offline_has_no_network_without_trusted_cache(tmp_path, monkeypatch, pinned):
    monkeypatch.setattr(release.urllib.request, "urlopen", lambda *a, **k: pytest.fail("network used"))
    monkeypatch.setattr(release, "__file__", str(tmp_path / "release.py"))
    metadata = {release.__version__: {"linux/arm64": {
        "url": "https://example.invalid/release.tar.gz", "sha256": "a" * 64,
    }}} if pinned else {}
    (tmp_path / "releases.json").write_text(json.dumps(metadata))
    message = "offline mode requires" if pinned else "no trusted release"
    with pytest.raises(release.BinaryUnavailable, match=message):
        release.acquire("arm64", {"cache_dir": str(tmp_path / "empty-cache")}, offline=True)


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
        with pytest.raises(release.BinaryInvalid, match="regular"):
            release.acquire("arm64", settings, offline=True)
    else:
        binary, sha = release.acquire("arm64", settings, offline=True)
        assert binary.read_bytes() == executable.read_bytes()
        assert sha == release.file_digest(executable)


@pytest.mark.parametrize('problem', ['', 'hash', 'platform', 'symlink', 'duplicate'])
def test_decoder_bundle_is_bound_to_archive_and_binary(tmp_path, monkeypatch, problem):
    package = tmp_path / 'package'
    package.mkdir()
    monkeypatch.setattr(release, '__file__', str(package / 'release.py'))
    cache = tmp_path / 'cache' / release.__version__ / 'linux-arm64'
    cache.mkdir(parents=True)
    binary = cache / 'oci-relay'
    sha = elf(binary)
    data = binary.read_bytes()
    manifest = {'format': 1, 'version': release.__version__, 'platform': 'linux/arm64',
                'capabilities': ['receiver-unpigz-v1'],
                'files': {name: {'sha256': sha, 'size': len(data)} for name in ('oci-relay', 'unpigz')}}
    if problem == 'hash':
        manifest['files']['unpigz']['sha256'] = '0' * 64
    if problem == 'platform':
        manifest['platform'] = 'linux/amd64'
    with tarfile.open(cache / 'release.tar.gz', 'w:gz') as tar:
        files = [('oci-relay', data), ('unpigz', data), ('bundle.json', json.dumps(manifest).encode()),
                 ('UNPIGZ_LICENSES.txt', b'licenses')]
        if problem == 'duplicate':
            files.append(('unpigz', data))
        for name, content in files:
            entry = tarfile.TarInfo(name)
            entry.size = len(content)
            if name == 'unpigz' and problem == 'symlink':
                entry.type, entry.linkname, entry.size = tarfile.SYMTYPE, 'oci-relay', 0
            tar.addfile(entry, io.BytesIO(content) if entry.isfile() else None)
    (package / 'releases.json').write_text(json.dumps({release.__version__: {'linux/arm64': {
        'url': 'https://example.invalid/release.tar.gz', 'sha256': release.file_digest(cache / 'release.tar.gz')}}}))
    if problem:
        with pytest.raises(release.BinaryInvalid):
            release.acquire_decoder('arm64', {}, binary)
    else:
        for _ in range(2):
            helper, observed = release.acquire_decoder('arm64', {}, binary)
            assert observed == sha and helper.read_bytes() == data
            helper.chmod(0o600)
            helper.write_bytes(b'tampered')
        assert (cache / 'UNPIGZ_LICENSES.txt').read_bytes() == b'licenses'


def test_development_decoder_requires_explicit_opt_in(tmp_path):
    binary = tmp_path / 'oci-relay'
    elf(binary)
    helper = tmp_path / 'unpigz'
    sha = elf(helper)
    settings = {'development_binary': str(binary)}
    assert release.acquire_decoder('arm64', settings, binary) is None
    settings['development_unpigz'] = str(helper)
    assert release.acquire_decoder('arm64', settings, binary) == (helper, sha)


@pytest.fixture
def download_settings(tmp_path, monkeypatch):
    monkeypatch.setattr(release, '__file__', str(tmp_path / 'release.py'))
    (tmp_path / 'releases.json').write_text(json.dumps({release.__version__: {'linux/arm64': {
        'url': 'https://example.invalid/release.tar.gz', 'sha256': 'a' * 64,
    }}}))
    return {'cache_dir': str(tmp_path / 'cache')}


@pytest.mark.parametrize('during_read', [False, True])
@pytest.mark.parametrize('error', [
    urllib.error.URLError('DNS lookup failed'), TimeoutError('timed out'),
    ConnectionResetError('connection reset'),
    urllib.error.HTTPError('https://example.invalid/release.tar.gz', 503, 'unavailable', {}, None),
    http.client.IncompleteRead(b'partial archive', 100),
])
def test_network_failures_are_unavailable_and_clean_partial_downloads(
    download_settings, tmp_path, monkeypatch, during_read, error,
):
    class Response(io.BytesIO):
        url = 'https://example.invalid/release.tar.gz'

        def read(self, size):
            chunk = super().read(size)
            if chunk:
                return chunk
            raise error

    response = Response(b'partial archive')

    def download(*a, **kw):
        if not during_read:
            raise error
        return response

    monkeypatch.setattr(release.urllib.request, 'urlopen', download)
    with pytest.raises(release.BinaryUnavailable, match='download unavailable'):
        release.acquire('arm64', download_settings, offline=False)
    assert not list((tmp_path / 'cache').rglob('release.tar.gz'))
    assert not list((tmp_path / 'cache').rglob('.download-*'))
    assert not list((tmp_path / 'cache').rglob('oci-relay'))
    if during_read:
        assert response.closed


@pytest.mark.parametrize('wrapped', [False, True])
def test_certificate_failure_is_not_download_unavailability(download_settings, monkeypatch, wrapped):
    error = ssl.SSLCertVerificationError('untrusted certificate')
    if wrapped:
        error = urllib.error.URLError(error)

    def download(*a, **kw):
        raise error

    monkeypatch.setattr(release.urllib.request, 'urlopen', download)
    with pytest.raises(release.BinaryInvalid, match='certificate'):
        release.acquire('arm64', download_settings, offline=False)


@pytest.mark.parametrize('problem', ['checksum', 'redirect', 'oversize', 'disk'])
def test_invalid_downloads_and_local_io_errors_do_not_allow_fallback(
    download_settings, tmp_path, monkeypatch, problem,
):
    response = io.BytesIO(b'untrusted archive')
    response.url = 'http://example.invalid/archive' if problem == 'redirect' else 'https://example.invalid/archive'
    monkeypatch.setattr(release.urllib.request, 'urlopen', lambda *a, **kw: response)
    if problem == 'oversize':
        monkeypatch.setattr(release, 'MAX_ARCHIVE', 1)
    if problem == 'disk':
        real_fdopen = release.os.fdopen

        class FullDisk:
            def __init__(self, fd, mode):
                self.stream = real_fdopen(fd, mode)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.stream.close()

            def write(self, chunk):
                raise OSError('No space left on device')

        monkeypatch.setattr(release.os, 'fdopen', FullDisk)
    expected = OSError if problem == 'disk' else release.BinaryInvalid
    with pytest.raises(expected):
        release.acquire('arm64', download_settings, offline=False)
    assert response.closed
    assert not list((tmp_path / 'cache').rglob('.download-*'))


@pytest.mark.parametrize('offline', [False, True])
def test_corrupt_cached_archive_never_falls_back(download_settings, tmp_path, monkeypatch, offline):
    cache = tmp_path / 'cache' / release.__version__ / 'linux-arm64'
    cache.mkdir(parents=True)
    (cache / 'release.tar.gz').write_bytes(b'corrupt')
    monkeypatch.setattr(release.urllib.request, 'urlopen', lambda *a, **kw: pytest.fail('network used'))
    with pytest.raises(release.BinaryInvalid, match='cached release archive checksum'):
        release.acquire('arm64', download_settings, offline=offline)
