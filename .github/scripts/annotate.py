#!/usr/bin/env python3
"""Re-emit the tail of a step's output (and container state/logs) as GitHub annotations.

Annotations are readable through the check-runs API even when raw job logs are not, so this
makes a red CI run debuggable from anywhere.

  annotate.py --title "lab01 drill" --log /tmp/drill.log --lines 80 --ps
  annotate.py --title "lab01 logs" --services kafka-1 kafka-2 --service-lines 40
  annotate.py --level notice --title "sample report" --file report.md --b64

Run from the directory that holds the docker-compose.yml when using --ps or --services. The compose
command comes from $COMPOSE (set by scripts/lib/engine.sh), so this works for Docker and Podman.
--b64 publishes a whole file base64-encoded, so it can be rebuilt byte for byte from the annotations.
"""
import argparse
import base64
import os
import re
import shlex
import subprocess

CHUNK = 3800        # keep each annotation message comfortably under GitHub's size cap
MAX_PER_STEP = 10   # GitHub keeps at most 10 annotations of each level per step
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
PROBLEM = re.compile(r"ERROR|WARN|Exception|FATAL")
COMPOSE = shlex.split(os.environ.get("COMPOSE") or
                      ("podman compose" if os.environ.get("CONTAINER_ENGINE") == "podman" else "docker compose"))


def escape_data(s: str) -> str:
    return s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def escape_prop(s: str) -> str:
    return escape_data(s).replace(":", "%3A").replace(",", "%2C")


def run(cmd):
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return (out.stdout + out.stderr).strip() or "(no output)"
    except Exception as exc:  # never let the diagnostics step itself hide the real failure
        return f"(could not run {' '.join(cmd)}: {exc})"


def chunks(text: str, size: int = CHUNK):
    """Split on line boundaries into pieces no longer than size characters."""
    out, cur = [], ""
    for line in text.splitlines():
        line = line[: size - 1]
        if len(cur) + len(line) + 1 > size:
            out.append(cur)
            cur = ""
        cur += line + "\n"
    if cur:
        out.append(cur)
    return out or ["(empty)"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", required=True)
    ap.add_argument("--log")
    ap.add_argument("--lines", type=int, default=80)
    ap.add_argument("--ps", action="store_true", help="include `$COMPOSE ps -a`")
    ap.add_argument("--file", help="with --b64: publish this whole file, base64-encoded")
    ap.add_argument("--b64", action="store_true")
    ap.add_argument("--services", nargs="*", default=[])
    ap.add_argument("--service-lines", type=int, default=40)
    ap.add_argument("--level", choices=["error", "notice"], default="error",
                    help="notice: record the output of a passing demo, so a green run can be checked too")
    args = ap.parse_args()

    blocks = []  # (title, text) in priority order
    if args.b64:
        with open(args.file, "rb") as f:
            data = base64.b64encode(f.read()).decode()
        parts = [data[i:i + CHUNK] for i in range(0, len(data), CHUNK)] or [""]
        for i, part in enumerate(parts, 1):
            blocks.append((f"{args.title}: base64 {i}/{len(parts)}", part))
    if args.log:
        if os.path.exists(args.log):
            with open(args.log, errors="replace") as f:
                lines = ANSI.sub("", f.read()).splitlines()[-args.lines:]
            text = "\n".join(lines) or "(log file is empty)"
        else:
            text = f"(log file {args.log} not found: the step may have failed before writing it)"
        parts = chunks(text)
        for i, part in enumerate(parts, 1):
            blocks.append((f"{args.title}: output tail {i}/{len(parts)}", part))
    if args.ps:
        blocks.append((f"{args.title}: {' '.join(COMPOSE)} ps", run(COMPOSE + ["ps", "-a"])))
    problems = []
    for svc in args.services:
        logs = ANSI.sub("", run(COMPOSE + ["logs", "--no-color", "--tail", "600", svc])).splitlines()
        blocks.append((f"{args.title}: logs {svc}", "\n".join(logs[-args.service_lines:])[-CHUNK:]))
        bad = [l for l in logs if PROBLEM.search(l)][-15:]
        if bad:
            problems.append((f"{args.title}: {svc} ERROR/WARN lines", "\n".join(bad)[-CHUNK:]))
    blocks += problems

    if len(blocks) > MAX_PER_STEP:
        blocks = blocks[: MAX_PER_STEP - 1] + [(f"{args.title}: truncated",
                                                 f"{len(blocks) - MAX_PER_STEP + 1} more blocks not shown")]
    for title, text in blocks:
        print(f"::{args.level} title={escape_prop(title)}::{escape_data(text)}", flush=True)


if __name__ == "__main__":
    main()
