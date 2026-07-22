#!/usr/bin/env python3
"""Pushes synthetic sample log data into a local Loki instance for demo/testing
purposes, via /loki/api/v1/push. See README.md "Live demo" section.

Usage:
    docker run -d --name loki-demo -p 3100:3100 grafana/loki:3.1.0 \
        -config.file=/etc/loki/local-config.yaml

    # One-shot: push 300 entries spanning the last ~60s, then exit.
    python3 scripts/push_logs.py

    # Continuous: keep pushing a fresh small batch every few seconds so Grafana
    # panels never go stale from Loki's default query-window lookback. Ctrl-C
    # to stop; run with `&` or nohup to leave it running in the background.
    python3 scripts/push_logs.py --continuous
    python3 scripts/push_logs.py --continuous --interval 5 --batch-size 20
"""

import argparse
import json
import random
import time
import urllib.request

BASE = "http://localhost:3100"

LEVELS_AND_LINES = {
    "info": [
        "request completed in {ms}ms",
        "cache hit for key user:{n}",
        "health check ok",
        "processed batch of {n} items",
    ],
    "warn": [
        "slow query took {ms}ms",
        "retrying request, attempt {n}",
        "connection pool at {n}% capacity",
    ],
    "error": [
        "panic: nil pointer dereference in handler",
        "connection reset by peer",
        "failed to write to database: timeout",
        "panic: index out of range [{n}] with length {n2}",
    ],
}

PODS = ["myapp-7d9f8c-abcde", "myapp-7d9f8c-fghij", "myapp-7d9f8c-klmno"]
ENVS = ["prod", "staging"]


def build_batch(n_entries: int, step_ns: int, now_ns: int) -> dict[tuple[str, str, str, str], list[list[str]]]:
    """Generates n_entries synthetic log lines spanning the n_entries*step_ns
    window immediately before now_ns, grouped by (job, level, env, pod) stream."""
    streams: dict[tuple[str, str, str, str], list[list[str]]] = {}

    for i in range(n_entries):
        ts_ns = now_ns - (n_entries - i) * step_ns
        level = random.choices(["info", "warn", "error"], weights=[70, 20, 10])[0]
        template = random.choice(LEVELS_AND_LINES[level])
        line = template.format(
            ms=random.randint(1, 500), n=random.randint(1, 100), n2=random.randint(1, 50)
        )
        env = random.choice(ENVS)
        pod = random.choice(PODS)
        job = "myapp"

        key = (job, level, env, pod)
        streams.setdefault(key, []).append([str(ts_ns), line])

    return streams


def push_batch(streams: dict[tuple[str, str, str, str], list[list[str]]]) -> int:
    """Pushes one batch to Loki. Returns the HTTP status code."""
    payload = {
        "streams": [
            {"stream": {"job": job, "level": level, "env": env, "pod": pod}, "values": values}
            for (job, level, env, pod), values in streams.items()
        ]
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{BASE}/loki/api/v1/push",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        return resp.status


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--continuous", action="store_true",
        help="keep pushing a fresh batch every --interval seconds instead of exiting after one push",
    )
    parser.add_argument(
        "--interval", type=float, default=5.0,
        help="seconds between pushes in --continuous mode (default: 5)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=None,
        help="entries per push; default is 300 for a one-shot push, 30 in --continuous mode",
    )
    args = parser.parse_args()

    random.seed(42)
    step_ns = 200_000_000  # 200ms apart within a batch

    if not args.continuous:
        n_entries = args.batch_size or 300
        streams = build_batch(n_entries, step_ns, time.time_ns())
        status = push_batch(streams)
        print("push status:", status)
        print(f"pushed {n_entries} entries across {len(streams)} streams")
        return

    n_entries = args.batch_size or 30
    print(f"pushing {n_entries} fresh entries every {args.interval}s — Ctrl-C to stop")
    try:
        while True:
            streams = build_batch(n_entries, step_ns, time.time_ns())
            status = push_batch(streams)
            print(f"[{time.strftime('%H:%M:%S')}] push status {status}: "
                  f"{n_entries} entries across {len(streams)} streams")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    main()
