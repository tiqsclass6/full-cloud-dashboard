# **Runbook: ops agent**

Alerts from `helm/charts/observability/rules/ai-ops.yaml`. They carry `ops_agent="skip"`, so they go to people (Grafana, Alertmanager), never to the agent. Design: [**docs/ai-ops.md**](../ai-ops.md).

## **OpsAgentComponentDown**

**Meaning:** `ops-agent`, `ops-control` or `mcp-platform-tools` (namespace `ai-ops`) has not been scraped for 10 minutes.
**Impact:** `ops-agent` down: alerts are not investigated. `ops-control` down: nothing can be approved or run, and the agent's results are lost (it retries nothing). `mcp-platform-tools` down: investigations fail.
**Diagnose:** `kc -n ai-ops get pods`; `kc -n ai-ops logs deploy/<name>`. `ops-control` exits at start if Postgres is unreachable or `ACTION_SIGNING_KEY` is shorter than 32 characters.
**Fix:** a missing Secret (`ai-ops-internal`, `postgres-credentials`) is recreated by `bash local/scripts/deploy-ai-ops.sh`; on AWS check the ExternalSecret status.

## **OpsAgentInvestigationsFailing**

**Meaning:** two or more investigations in the last hour ended without a diagnosis.
**Diagnose:** the outcome is in the agent's log line `{"event": "investigation", ... "outcome": ...}` (Loki: `{namespace="ai-ops", container="ops-agent"} |= "investigation"`) and in `ops.investigations` (`outcome`, `error`):

- `error` with `AccessDenied` or `ExpiredToken`: the model credentials expired. Locally they last one hour: `bash local/scripts/ai-creds.sh`. On AWS, check the IRSA role.
- `error` with `not available for this account`: Bedrock model access is not enabled for the account.
- `refused`: the model declined (the fallback model declined too). Read the trigger; a prompt-injection attempt in an alert annotation can cause this.
- `invalid_output`: the answer did not match the schema; report it with the investigation id.
**Fix:** as above. Alerts keep working without the agent; only the Slack diagnosis is missing.

## **OpsAgentAlertsDropped**

**Meaning:** more alert groups arrived than the hourly budget (`agent.maxInvestigationsPerHour`) allows; some were not investigated.
**Impact:** none on the platform; the skipped alerts are still in Alertmanager and Grafana.
**Fix:** usually an incident with many alerts at once: look at Alertmanager directly. If the budget is too low for normal operation, raise it in `helm/charts/ai-ops/values.yaml` (each investigation costs model tokens).

## **OpsControlActionFailed**

**Meaning:** an action a person approved did not complete.
**Diagnose:** the error is in the Slack thread and in `ops.proposals.result` / `ops.audit_log`. A Kubernetes `403` means the action is missing from `helm/charts/ai-ops-access` (the RBAC chart).
**Fix:** nothing retries an action automatically. Fix the cause, then let the agent propose it again on the next alert, or run the runbook step by hand.

## **Approving from a terminal**

When Slack is not configured (or unavailable), proposals are approved with `kubectl exec`, so the approver is whoever may exec into `ops-control`:

```bash
kubectl -n ai-ops exec deploy/ops-control -- python -m ops_agent proposals --status pending
kubectl -n ai-ops exec deploy/ops-control -- python -m ops_agent approve <id> --hash <first 12 characters> --approver <your name>
```

The hash binds the approval to the exact action and parameters listed; add `--reject` to reject.
