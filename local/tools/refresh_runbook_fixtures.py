"""Refresh the recorded search_runbooks answers in the eval fixtures from the current docs/runbooks.

Fixtures record cluster state at the time of an incident; runbooks are documentation and change with the repo,
so after a runbook edit the replayed answers should match what the tool server would return today.
Usage: python -I local/tools/refresh_runbook_fixtures.py [--check]
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "apps" / "mcp-platform-tools"))
from platform_tools.runbooks import RunbookIndex  # noqa: E402

LIMIT = 3  # platform_tools.tools.Tools.search_runbooks


def main() -> int:
    check = "--check" in sys.argv[1:]
    index = RunbookIndex(str(ROOT / "docs" / "runbooks"))
    stale = []
    for path in sorted((ROOT / "evals" / "agent-scenarios" / "fixtures").glob("*.json")):
        fixture = json.loads(path.read_text(encoding="utf-8"))
        changed = 0
        for r in fixture["responses"]:
            if r["tool"] != "search_runbooks" or r.get("is_error"):
                continue
            # The same serialisation as the tool server (platform_tools/server.py).
            fresh = json.dumps({"results": index.search(r["arguments"]["query"], limit=LIMIT)}, separators=(",", ":"),
                               default=str)
            if fresh != r["result"]:
                r["result"], changed = fresh, changed + 1
        if changed:
            stale.append(f"{path.name}: {changed} answer(s)")
            if not check:
                path.write_text(json.dumps(fixture, indent=1, ensure_ascii=False), encoding="utf-8")  # as recorded
    for line in stale:
        print(("stale " if check else "refreshed ") + line)
    return 1 if check and stale else 0


if __name__ == "__main__":
    sys.exit(main())
