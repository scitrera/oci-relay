#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
"""Build a target-native, source-pinned unpigz and populate a release bundle.

Linux builds musl and zlib statically without installing host packages. macOS
uses its system runtime and a static zlib. No cross-architecture guesses.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tarfile
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def command(args, cwd, env):
    subprocess.run(args, cwd=cwd, env=env, check=True)


def source(name, pin, cache, work):
    archive = cache / (name + '-' + pin['version'] + '.tar.gz')
    if not archive.is_file() or sha(archive) != pin['sha256']:
        with urllib.request.urlopen(pin['url'], timeout=60) as response:
            if not response.url.startswith('https://'):
                raise ValueError('dependency redirect requires HTTPS')
            data = response.read(32 << 20)
        if hashlib.sha256(data).hexdigest() != pin['sha256']:
            raise ValueError('source checksum mismatch: ' + name)
        archive.write_bytes(data)
    with tarfile.open(archive) as tar:
        # All archives have a single known top-level directory. Data filtering
        # also rejects paths and links escaping the extraction directory.
        prefix = name + '-' + pin['version']
        if any(e.name.split('/')[0] != prefix for e in tar.getmembers()):
            raise ValueError('unexpected source archive layout')
        tar.extractall(work, filter='data')
    return work / prefix


def smoke(executable):
    data = b'OCI Relay bundled decoder validation\0' * 32768
    stream = gzip.compress(data[:len(data)//2], mtime=0) + gzip.compress(data[len(data)//2:], mtime=0)
    env = dict(os.environ)
    env.pop('GZIP', None)
    env.pop('PIGZ', None)
    run = subprocess.run([str(executable), '-d', '-c'], input=stream, capture_output=True, env=env, timeout=30)
    if run.returncode or run.stdout != data:
        raise ValueError('decoder round trip failed')
    corrupt = bytearray(stream)
    corrupt[-8] ^= 1
    for payload in (stream[:-7], corrupt):
        run = subprocess.run([str(executable), '-d', '-c'], input=payload, capture_output=True, env=env, timeout=30)
        if not run.returncode:
            raise ValueError('decoder accepted corrupt/truncated gzip')


def write_manifest(stage, args, pins):
    files = {name: {'sha256': sha(stage / name), 'size': (stage / name).stat().st_size}
             for name in ('oci-relay', 'unpigz')}
    dependencies = {name: pin for name, pin in pins.items() if name != 'musl' or args.os == 'linux'}
    manifest = {'format': 1, 'version': args.version, 'platform': args.os + '/' + args.arch,
                'capabilities': ['receiver-unpigz-v1'], 'files': files, 'dependencies': dependencies,
                'build_script_sha256': sha(Path(__file__))}
    (stage / 'bundle.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
    print(json.dumps(manifest, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', type=Path, default=os.environ.get('BUNDLE_DIR'))
    parser.add_argument('--os', default=os.environ.get('GOOS', platform.system().lower()))
    parser.add_argument('--arch', default=os.environ.get('GOARCH', {'aarch64': 'arm64', 'arm64': 'arm64', 'x86_64': 'amd64'}.get(platform.machine())))
    parser.add_argument('--version', default=os.environ.get('VERSION', 'development'))
    parser.add_argument('--reuse', action='store_true', help='reuse a verified native development helper')
    parser.add_argument('--cache', type=Path, default=ROOT / 'build/native-sources')
    args = parser.parse_args()
    actual = (platform.system().lower(), {'aarch64': 'arm64', 'arm64': 'arm64', 'x86_64': 'amd64'}.get(platform.machine()))
    if (args.os, args.arch) != actual or actual not in {('linux', 'amd64'), ('linux', 'arm64'), ('darwin', 'arm64')}:
        raise ValueError('build and test helpers on their target platform')
    if args.stage is None:
        parser.error('--stage is required')
    stage = args.stage.resolve()
    if not (stage / 'oci-relay').is_file():
        raise ValueError('stage must contain the relay executable')
    args.cache.mkdir(parents=True, exist_ok=True)
    pins = json.loads((ROOT / 'scripts/native-dependencies.json').read_text())
    if args.reuse and (stage / 'bundle.json').is_file() and (stage / 'unpigz').is_file() and (stage / 'UNPIGZ_LICENSES.txt').is_file():
        previous = json.loads((stage / 'bundle.json').read_text())
        dependencies = {name: pin for name, pin in pins.items() if name != 'musl' or args.os == 'linux'}
        if (previous.get('platform') == args.os + '/' + args.arch
                and previous.get('dependencies') == dependencies
                and previous.get('build_script_sha256') == sha(Path(__file__))
                and previous.get('files', {}).get('unpigz', {}).get('sha256') == sha(stage / 'unpigz')):
            smoke(stage / 'unpigz')
            write_manifest(stage, args, pins)
            return
    jobs = str(min(os.cpu_count() or 2, 8))
    with tempfile.TemporaryDirectory(prefix='oci-relay-native-') as temporary:
        work = Path(temporary)
        env = dict(os.environ, LC_ALL='C', SOURCE_DATE_EPOCH='1692403200')
        env.pop('CFLAGS', None)
        env.pop('LDFLAGS', None)
        licenses = []
        cc = env.get('CC', 'cc')
        if args.os == 'linux':
            musl = source('musl', pins['musl'], args.cache, work)
            prefix = work / 'musl-install'
            command(['./configure', '--prefix=' + str(prefix), '--disable-shared'], musl, env)
            command(['make', '-j' + jobs], musl, env)
            command(['make', 'install'], musl, env)
            cc = str(prefix / 'bin/musl-gcc')
            env['LDFLAGS'] = '-static'
            licenses.append(('musl ' + pins['musl']['version'], (musl / 'COPYRIGHT').read_text()))
        zlib = source('zlib', pins['zlib'], args.cache, work)
        env['CC'] = cc
        env['CFLAGS'] = '-O3 -ffile-prefix-map=' + str(work) + '=.'
        if args.os == 'darwin':
            env['MACOSX_DEPLOYMENT_TARGET'] = '12.0'
        command(['./configure', '--static'], zlib, env)
        command(['make', '-j' + jobs], zlib, env)
        command(['make', 'test'], zlib, env)
        licenses.append(('zlib ' + pins['zlib']['version'], (zlib / 'LICENSE').read_text()))
        pigz = source('pigz', pins['pigz'], args.cache, work)
        output = stage / 'unpigz'
        output.unlink(missing_ok=True)
        # NOZOPFLI omits an unused compression-only dependency. Inflate uses
        # the same zlib implementation as the normal upstream build.
        command([cc, '-O3', '-DNOZOPFLI', '-ffile-prefix-map=' + str(work) + '=.',
                 '-I' + str(zlib), 'pigz.c', 'yarn.c', 'try.c', str(zlib / 'libz.a'),
                 '-pthread', '-lm', *(['-static'] if args.os == 'linux' else []), '-o', str(output)], pigz, env)
        command(['strip', str(output)], pigz, env)
        if args.os == 'linux':
            headers = subprocess.check_output(['readelf', '-l', '-d', str(output)], text=True)
            if 'INTERP' in headers or '(NEEDED)' in headers:
                raise ValueError('Linux decoder must be statically linked')
        else:
            linked = subprocess.check_output(['otool', '-L', str(output)], text=True).splitlines()[1:]
            if any(not line.strip().startswith(('/usr/lib/', '/System/Library/')) for line in linked):
                raise ValueError('unexpected macOS runtime dependency')
            command(['codesign', '--force', '--sign', '-', str(output)], pigz, env)
        output.chmod(0o555)
        smoke(output)
        licenses.append(('pigz ' + pins['pigz']['version'] + ' (NOZOPFLI build)', (pigz / 'README').read_text()))
        (stage / 'UNPIGZ_LICENSES.txt').write_text('\n\n'.join(title + '\n\n' + body for title, body in licenses))
    write_manifest(stage, args, pins)


if __name__ == '__main__':
    main()
