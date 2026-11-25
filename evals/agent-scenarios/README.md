# **Ops agent eval scenarios**

Recorded incidents for the ops agent's eval suite (`apps/ai-ops/ops_agent/evals.py`, built on pydantic-evals). Overview and results: [**docs/ai-ops.md**](../../docs/ai-ops.md#evals).

| **Path**                    | **Content**                                                                                                                |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| **`scenarios/<name>.yaml`** | What happened, and the accepted root-cause categories and actions, the forbidden actions, the tools that must be consulted |
| **`alerts/<name>.json`**    | The Alertmanager webhook payload, as Alertmanager had it when the alert fired                                              |
| **`fixtures/<name>.json`**  | Every read-only tool's answer at that moment (and the tool definitions), recorded through the real MCP server              |
| **`results/<run>/`**        | `report.json` and one transcript per run (`make ai-eval`)                                                                  |

## **How the fixtures were recorded**
- Chaos experiments: `CAPTURE=1 bash local/scripts/chaos-run.sh <experiment>` records the fixture 45 seconds after the experiment's trigger alert fires (`local/scripts/ai-capture.sh`).
- `resolved-staleness`: `bash local/scripts/ai-capture-staleness.sh` suspends the Flink job until `IcebergTableStale` fires, records the alert, resumes the job, and records the tools once the table is fresh again.
- `compaction-overdue`: `bash local/scripts/ai-capture.sh compaction-overdue IcebergCompactionOverdue,IcebergSmallFilesHigh` while the real alert fires (the compaction schedule is suspended on kind, so it fires about six hours after the last compaction).
- `prompt-injection`: `python -I local/tools/derive_injection.py` copies `compaction-overdue` and plants instructions in the alert annotation, a Kubernetes event and log lines. Everything else is the real recording.
- After a live investigation, `bash local/scripts/ai-capture.sh --merge <scenario>` adds the agent's own tool calls (recorded at the time) so its queries replay exactly.
- Runbooks are documentation, not incident state: after a runbook edit, `python -I local/tools/refresh_runbook_fixtures.py` rewrites the recorded `search_runbooks` answers from `docs/runbooks` (`--check` only reports stale ones).

## **Replay**
Calls are matched exactly first, then by the arguments that identify what a call is about (namespace, pod, query), ignoring time windows and limits. A call with no recording gets the same error a failed data source would return, with no hint about the fixture; the report counts these `replay_misses`, so a scenario that needs a fresh capture is visible.

Fixtures contain cluster state from the local kind cluster only (pod names, metrics, log lines), already redacted by the tool server.
