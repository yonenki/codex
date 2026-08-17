#!/usr/bin/env python3
"""Build the agent-collab Codex runtime with platform-correct migration line endings.

Why this exists:
  sqlx::migrate! embeds the raw bytes of codex-rs/state/*_migrations/*.sql into the
  binary and verifies them against the checksums stored in the user's state DBs
  (~/.codex/state_5.sqlite etc.). Official Codex *Windows* releases embed those files
  with CRLF line endings, while official Linux/macOS releases embed LF. A runtime
  built with the wrong line endings fails at startup with:

      migration N was previously applied but has been modified

  and the agent-collab server cannot start any agent. The SQL text is identical;
  only the line endings differ, so the fix is to normalize the migration files to
  the platform's convention before compiling, and back to LF afterwards so the
  working tree stays clean.

Usage:
  python scripts/build_agent_collab_runtime.py [--with-code-mode-host] [--eol crlf|lf]

  --with-code-mode-host  Also build codex-code-mode-host. Off by default because
                         the v8 prebuilt archive for windows-x86_64 is currently
                         unpublished (rusty_v8 404), and the host does not touch
                         the state DBs, so an existing host binary stays valid.
  --eol                  Override the target line endings (default: crlf on
                         Windows, lf elsewhere).
"""

import argparse
import hashlib
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = REPO_ROOT / "codex-rs" / "state"
MIGRATION_DIRS = [
    "migrations",
    "logs_migrations",
    "goals_migrations",
    "memory_migrations",
    "queue_migrations",
    "thread_history_migrations",
]

# Known-good SHA-384 digests of migrations/0001_threads.sql, matching what the
# official Codex releases embed on each platform. Printed for cross-checking
# against `SELECT hex(checksum) FROM _sqlx_migrations WHERE version = 1`.
SENTINEL_FILE = STATE_DIR / "migrations" / "0001_threads.sql"
SENTINEL_SHA384 = {
    "crlf": "54bbd6f47905a4e4c674034575963d82da7b534e66e9a37a81ec2afb6a4b56ce6de9b3ecf3032796a800f650239847d4",
    "lf": "627ef19164c9bb298a0cd99945981c9b7bda3d9e6cf12eb35145e3b1d3bf7cf8740f0dbaa0b475185fc2993397078049",
}


def migration_files():
    for name in MIGRATION_DIRS:
        yield from sorted((STATE_DIR / name).glob("*.sql"))


def normalize_eol(eol: str) -> int:
    """Rewrite every migration file to the target EOL. Returns changed count."""
    changed = 0
    for path in migration_files():
        original = path.read_bytes()
        text = original.decode("utf-8").replace("\r\n", "\n")
        if eol == "crlf":
            text = text.replace("\n", "\r\n")
        updated = text.encode("utf-8")
        if updated != original:
            path.write_bytes(updated)
            changed += 1
    return changed


def verify_eol(eol: str) -> None:
    """Fail loudly if any migration file does not use the target EOL."""
    for path in migration_files():
        data = path.read_bytes()
        if eol == "crlf":
            if data.replace(b"\r\n", b"").find(b"\n") != -1:
                raise SystemExit(f"error: {path} still contains bare LF after CRLF conversion")
        elif b"\r" in data:
            raise SystemExit(f"error: {path} still contains CR after LF conversion")


def print_sentinel() -> None:
    digest = hashlib.sha384(SENTINEL_FILE.read_bytes()).hexdigest()
    matches = [name for name, known in SENTINEL_SHA384.items() if digest == known]
    label = f"matches official {matches[0]} build" if matches else "matches NO known official build"
    print(f"[migration-eol] 0001_threads.sql sha384={digest} ({label})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--with-code-mode-host", action="store_true")
    parser.add_argument("--eol", choices=["crlf", "lf"], default=None)
    args = parser.parse_args()

    target_eol = args.eol or ("crlf" if os.name == "nt" else "lf")
    packages = ["codex-cli"] + (["codex-code-mode-host"] if args.with_code_mode_host else [])

    changed = normalize_eol(target_eol)
    verify_eol(target_eol)
    print(f"[migration-eol] normalized {changed} file(s) to {target_eol.upper()} for this build")
    print_sentinel()

    command = ["cargo", "build", "--release"]
    for package in packages:
        command += ["-p", package]
    print(f"[migration-eol] running: {' '.join(command)}")
    try:
        result = subprocess.run(command, cwd=REPO_ROOT / "codex-rs")
    finally:
        # The repository's canonical form is LF; always restore it so the
        # conversion never leaks into commits or surprises the next build.
        restored = normalize_eol("lf")
        if restored:
            print(f"[migration-eol] restored {restored} file(s) to LF")
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
