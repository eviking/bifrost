#!/usr/bin/env python3
"""Pushes synthetic sample log data into a local Loki instance for demo/testing
purposes, via /loki/api/v1/push. See README.md "Live demo" section.

Usage:
    docker run -d --name loki-demo -p 3100:3100 grafana/loki:3.1.0 \
        -config.file=/etc/loki/local-config.yaml
    python3 scripts/push_logs.py
"""

import json
import random
import time
import urllib.request

BASE = "http://localhost:3100"

now_ns = time.time_ns()

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


def main():
    random.seed(42)

    n_entries = 300
    step_ns = 200_000_000  # 200ms apart, going backwards from now

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
        print("push status:", resp.status)

    print(f"pushed {n_entries} entries across {len(streams)} streams")


if __name__ == "__main__":
    main()
