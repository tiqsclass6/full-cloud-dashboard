#!/usr/bin/env python3
"""Summarise a Java Flight Recording into the numbers used in docs/performance.

Usage: jfr_summary.py <recording.jfr> --label NAME --gc g1|zgc --load '<loadgen RESULT json>' [--window SECONDS]
"""
import argparse
import collections
import datetime as dt
import json
import os
import re
import shutil
import statistics
import subprocess
import sys

DURATION = re.compile(r"^PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?$")


def seconds(iso):
    m = DURATION.match(iso)
    if not m:
        raise ValueError(f"unexpected duration {iso!r}")
    h, mi, s = (float(g) if g else 0.0 for g in m.groups())
    return h * 3600 + mi * 60 + s


def timestamp(iso):
    # Python parses at most microseconds; JFR prints nanoseconds.
    return dt.datetime.fromisoformat(re.sub(r"(\.\d{6})\d*Z$", r"\1+00:00", iso))


def find_jfr():
    found = shutil.which("jfr")
    if found:
        return found
    home = os.environ.get("JAVA_HOME")
    candidates = [os.path.join(home, "bin", "jfr")] if home else []
    candidates += [r"C:\Program Files\OpenJDK\jdk-25\bin\jfr.exe", "/usr/lib/jvm/default-java/bin/jfr"]
    for c in candidates:
        if os.path.exists(c):
            return c
    sys.exit("cannot find the `jfr` tool; set JAVA_HOME to a JDK")


def events(jfr, recording, *types):
    out = subprocess.run([jfr, "print", "--json", "--events", ",".join(types), recording],
                         check=True, capture_output=True, text=True).stdout
    return json.loads(out)["recording"]["events"] if out.strip() else []


def percentile(values, p):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(p / 100 * (len(ordered) - 1))))]


def top_frame(event):
    frames = (event["values"].get("stackTrace") or {}).get("frames") or []
    if not frames:
        return "(no stack)"
    method = frames[0]["method"]
    return f'{method["type"]["name"]}.{method["name"]}'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("recording")
    ap.add_argument("--label", required=True)
    ap.add_argument("--gc", required=True)
    ap.add_argument("--load", default="{}")
    ap.add_argument("--window", type=float)
    args = ap.parse_args()

    load = json.loads(args.load or "{}")
    window = args.window or float(load.get("measured_seconds", 30))
    jfr = find_jfr()

    gcs = events(jfr, args.recording, "jdk.GarbageCollection")
    heap = events(jfr, args.recording, "jdk.GCHeapSummary")
    cpu = events(jfr, args.recording, "jdk.CPULoad")
    samples = events(jfr, args.recording, "jdk.ExecutionSample")
    allocs = events(jfr, args.recording, "jdk.ObjectAllocationSample")

    latest = max(timestamp(e["values"]["startTime"]) for e in gcs + cpu)
    start = latest - dt.timedelta(seconds=window)

    def in_window(evts):
        return [e for e in evts if timestamp(e["values"]["startTime"]) >= start]

    gcs, heap, cpu, samples, allocs = map(in_window, (gcs, heap, cpu, samples, allocs))

    pauses = [seconds(e["values"]["sumOfPauses"]) * 1000 for e in gcs]
    longest = [seconds(e["values"]["longestPause"]) * 1000 for e in gcs]
    after_gc = [e["values"]["heapUsed"] for e in heap if e["values"]["when"] == "After GC"]
    used = [e["values"]["heapUsed"] for e in heap]
    cpu_total = [e["values"]["jvmUser"] + e["values"]["jvmSystem"] for e in cpu]

    hot = collections.Counter(top_frame(e) for e in samples)
    alloc_by_site = collections.Counter()
    total_alloc = 0
    for e in allocs:
        weight = e["values"].get("weight", 0)
        total_alloc += weight
        alloc_by_site[top_frame(e)] += weight

    mib = 1024 * 1024
    summary = {
        "label": args.label,
        "gc": args.gc,
        "window_seconds": window,
        "load": load,
        "gc_stats": {
            "collections": len(gcs),
            "collections_per_second": round(len(gcs) / window, 2),
            "total_pause_ms": round(sum(pauses), 1),
            "pause_time_percent": round(sum(pauses) / (window * 1000) * 100, 3),
            "pause_ms": {"p50": round(percentile(longest, 50), 2), "p99": round(percentile(longest, 99), 2),
                         "max": round(max(longest, default=0), 2)},
        },
        "heap_mib": {
            "peak_used": round(max(used, default=0) / mib, 1),
            "live_after_gc_median": round(statistics.median(after_gc) / mib, 1) if after_gc else 0,
        },
        "cpu": {
            "jvm_mean_percent_of_machine": round(statistics.mean(cpu_total) * 100, 1) if cpu_total else 0,
            "jvm_max_percent_of_machine": round(max(cpu_total, default=0) * 100, 1),
        },
        "allocation": {
            "sampled_mib_per_second": round(total_alloc / mib / window, 1),
            "top_sites_percent": {k: round(v / total_alloc * 100, 1) for k, v in alloc_by_site.most_common(8)}
            if total_alloc else {},
        },
        "hot_methods_percent": {k: round(v / len(samples) * 100, 1) for k, v in hot.most_common(10)}
        if samples else {},
    }
    json.dump(summary, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
