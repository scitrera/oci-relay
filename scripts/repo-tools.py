#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Spark Arena
# SPDX-License-Identifier: Apache-2.0
"""Use the shared toolkit from a local checkout or its pinned Git revision."""
import os
from pathlib import Path
import subprocess
import sys

SOURCE = "git+https://github.com/scitrera/repo-tools.git@898be3f3408a0da7d5dee81d8752c06d2ac7bf23"
local = Path(os.environ.get("SCITRERA_REPO_TOOLS", "~/scitrera-repo-tools")).expanduser()
env = os.environ.copy()
if (local / "src/scitrera_repo_tools").is_dir():
    env["PYTHONPATH"] = str(local / "src") + os.pathsep + env.get("PYTHONPATH", "")
    command = [sys.executable, "-m", "scitrera_repo_tools", *sys.argv[1:]]
else:
    command = ["uvx", "--from", SOURCE, "repo-tools", *sys.argv[1:]]
raise SystemExit(subprocess.call(command, env=env, cwd=Path(__file__).resolve().parents[1]))
