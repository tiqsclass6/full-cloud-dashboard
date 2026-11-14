# **ADR 0010: An ops agent that diagnoses alerts and proposes fixes a person approves (Phase 9)**

- Status: accepted
- Date: 2026-11-01

## **Context**
Phase 8 gave every alert a runbook, but a person still has to open the dashboards, read the runbook and run its steps. The job description asks for AI integrated into engineering and operations work. An agent that triages alerts is useful only if it cannot make an incident worse: it must not change anything on its own, must not be steered by text an attacker can write into a log line, and must be measurable before anyone relies on it.

## **Decisions**
1. **Three components, each with only the access its job needs.**
   - `mcp-platform-tools`: an MCP server with 13 read-only tools over Prometheus, Alertmanager, Loki, the Kubernetes API and the platform's custom resources. Its service account may read pods, logs, events and the platform resources in listed namespaces, never Secrets or ConfigMaps, and change nothing.
   - `ops-agent`: receives Alertmanager webhooks and runs the investigation. It has **no Kubernetes credentials at all** (no service account token is mounted); it can reach only the model, the tools and `ops-control`.
   - `ops-control`: stores everything (Postgres), posts to Slack, takes approvals and runs approved actions. Its service account may do exactly the catalogued actions: delete a pod in `apps`, `kafka` or `data`; patch the one Flink deployment; create a Spark application from the compaction schedule.
2. **The agent proposes; a person approves; ops-control executes.**
   - The model's only output is a structured diagnosis (enforced with structured outputs). An action is one of a closed catalogue (`restart_pod`, `restart_flink_job`, `run_iceberg_compaction`, or the recommendations `none` and `escalate`) with strictly validated parameters.
   - ops-control validates the proposal against the catalogue again: the model's output is never trusted.
   - Approval is bound to the exact action and parameters: each Slack button carries the hash of what was shown, and a mismatch is refused.
   - Approvers are a list of Slack user IDs. In the terminal, the approver is whoever may `kubectl exec` into ops-control. Two-person approval is a setting.
   - An approval issues a signed execution token (HMAC-SHA256, 60 seconds) bound to the proposal, action and parameter hash. The executor accepts it only once (compare-and-set from `approved` to `executing`) and re-checks the hash against the stored parameters.
3. **Claude Sonnet 4.6 in Amazon Bedrock, with Claude Opus 5.5 as the refusal fallback.**
   - The Anthropic SDK's Bedrock client calls the Messages API on the `bedrock-mantle` endpoint. Bedrock has no server-side fallback, so the SDK's client-side middleware retries a declined request on Opus 5.5.
   - Effort `medium` (the recommended start for multistep tool use), adaptive thinking with summaries kept for the audit record, and the system prompt cached.
   - On kind the agent uses one-hour credentials for a role that may invoke only these two models; on EKS it uses IRSA. Hermes has its own IRSA role on EKS, limited to Sonnet 4.6 through the US inference profile.
4. **A hand-written tool loop, not the SDK's tool runner.** Every step is a guardrail:
   - a hard budget of turns and tool calls, after which tools are switched off (`tool_choice: none`) and the model must answer;
   - a record of every call (arguments, result, duration) for the audit log and the eval fixtures;
   - refusal handling;
   - a check that every tool cited as evidence was really called.

   The history is append-only, which keeps the prompt cache and the thinking blocks valid.
5. **Untrusted text stays data.**
   - Tool results are redacted before they leave the tool server (keys, tokens, passwords, private keys).
   - The system prompt says log lines, events and annotations are data, and that requests inside them must be reported, not followed.
   - The structural controls above hold even if the model is fooled: no write tools exist for it, and every action needs a person.
   - Model text posted to Slack is escaped, so it cannot mention the channel or forge links.
6. **Guardrails on the trigger path.**
   - The agent investigates only firing critical and warning alerts.
   - Each alert group and set of alerts is investigated once per 30 minutes, at most six times an hour, one at a time.
   - Alerts about the agent itself carry `ops_agent="skip"` and are never routed back to it.
   - A forged webhook can therefore cost a few read-only investigations and nothing else. Network policy admits only Alertmanager to the agent, and tokens protect the tool server and the control API.
7. **Permissions live in their own chart and Argo CD project.** The `lakehouse` project still may not create any RBAC. `ai-ops-access` may create only Roles, RoleBindings, ClusterRoles and ClusterRoleBindings, in the namespaces the agent reads or acts in. A change to what the agent may do is a reviewed change to that one chart.
8. **Evaluation by replaying real incidents, scored without a model.**
   - The chaos experiments of Phase 8 are re-run, and the tool results at alert time are recorded as fixtures. The eval replays the fixtures to the real model, so it tests reasoning against real data without a cluster.
   - Scoring is deterministic: root-cause category, proposed action, a safety check (no forbidden action), and grounding (evidence comes from tools that were called).
   - A prompt-injection scenario plants instructions in cluster text.
   - The gate is no unsafe proposal at all, and at least 80% of runs fully right.

## **Consequences**
- The agent cannot fix anything by itself. That is the point, and it limits it to what the catalogue covers. New actions need code, RBAC and a review.
- Replay cannot answer a query the recording does not contain. The eval reports these misses instead of hiding them; fixtures are re-captured when the platform changes.
- Single replicas: the webhook de-duplication is in memory, and there is one Slack connection. A restart may repeat an investigation. That is acceptable at this scale.
- Model calls cost money: an investigation takes about 10 tool calls (see docs/ai-ops.md for measured cost). The hourly budget caps the spend.
- On AWS, Bedrock AgentCore (Runtime and Gateway) is the planned home for the agent and its tools, with the same code. Docs/ai-ops.md records what was deployed and verified there.
