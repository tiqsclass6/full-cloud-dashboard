#!/usr/bin/env python3
"""Workflow infographics: one SVG per major flow; every number comes from a recorded run.

  workflows.py OUT_DIR      writes OUT_DIR/workflow-<name>.svg
"""
import os
import sys
from xml.sax.saxutils import escape

# One colour per layer, shared with architecture.py.
LAYERS = {
    "data": ("Data path", "#2563eb"),
    "process": ("Processing", "#0891b2"),
    "observe": ("Observability", "#ea580c"),
    "deliver": ("Delivery", "#16a34a"),
    "secure": ("Security", "#dc2626"),
    "ai": ("Agentic AI", "#7c3aed"),
    "aws": ("AWS", "#d97706"),
    "people": ("People", "#475569"),
}

WORKFLOWS = [
    {
        "name": "ingestion-exactly-once",
        "title": "Event to Iceberg, exactly once",
        "subtitle": "Each event lands in the table once, through client retries, pod kills and restarts",
        "steps": [
            ("data", "Client sends", "HTTP JSON batch with a client-chosen event_id; a retry reuses the same id"),
            ("data", "Ingestion service", "Java 21, virtual threads; Avro with the schema in Apicurio; answers 429 when in-flight work is full"),
            ("secure", "Kafka acknowledges", "acks=all over mutual TLS; 3 replicas, min ISR 2; the client gets 200 only once the write is durable"),
            ("process", "Flink de-duplicates", "keyBy(event_id) with 60-minute state TTL: a repeated id is dropped"),
            ("process", "Checkpoint every 10 s", "Kafka offsets and de-duplication state snapshot to S3; the sink stages Parquet files"),
            ("data", "Two-phase commit", "The Iceberg snapshot is committed only when the checkpoint completes; a restart re-commits safely"),
            ("data", "Readers see it once", "PyIceberg locally, Athena on AWS: row count equals distinct ids"),
        ],
        "stats": [
            ("24,000 = 24,000", "rows = distinct ids after a TaskManager kill and a savepoint restore", "data"),
            ("3,400", "duplicate sends and post-restart re-sends dropped", "process"),
            ("0 / 323,400", "events lost or duplicated across five chaos experiments", "secure"),
            ("5,000 = 5,000", "events counted once by Athena on EKS", "aws"),
        ],
        "source": "docs/flink-iceberg.md, docs/chaos/README.md, docs/aws.md",
    },
    {
        "name": "spark-maintenance",
        "title": "Iceberg maintenance and aggregates with Spark",
        "subtitle": "Streaming leaves many small files; scheduled Spark jobs fix that without pausing the stream",
        "steps": [
            ("process", "Flink appends", "One snapshot and a few small Parquet files per checkpoint"),
            ("observe", "Exporter watches", "iceberg-exporter: file sizes, snapshots, last compaction; alerts such as IcebergSmallFilesHigh, IcebergCompactionOverdue"),
            ("deliver", "Operator schedules", "ScheduledSparkApplication (Kubeflow Spark operator), Spark 4.1 pods on Kubernetes"),
            ("process", "Compact", "rewrite_data_files merges small files; commits while Flink keeps appending"),
            ("process", "Manifests and expiry", "Rewrite manifests, expire snapshots: keeps 72 h and at least 10 snapshots for time travel"),
            ("data", "Hourly aggregate", "events_hourly by hour, source and type with MERGE: re-runs and late data stay correct"),
        ],
        "stats": [
            ("70 → 1", "data files after compaction (2,501,500 rows unchanged)", "process"),
            ("1.68 s → 0.48 s", "median full-table scan, one run (±20% noise)", "observe"),
            ("3,501,500", "Kafka messages = table rows, all distinct, with compaction under 5,000 events/s", "data"),
            ("0", "Flink restarts while two compactions committed", "deliver"),
        ],
        "source": "docs/spark-iceberg.md",
    },
    {
        "name": "gitops-delivery",
        "title": "GitOps delivery",
        "subtitle": "Git is the only way in: Argo CD installs and keeps every operator and chart in line with it",
        "steps": [
            ("deliver", "Change in Git", "Helm charts plus layered values: base, then local, dev, staging, prod, aws"),
            ("deliver", "Check", "make helm-check renders every environment and validates the manifests"),
            ("deliver", "Publish", "make gitops-push mirrors the tree to in-cluster Gitea as a signed commit"),
            ("deliver", "Argo CD syncs", "Root app of apps, ApplicationSets for operators and platform; picks the change up within a minute"),
            ("secure", "Scoped and admitted", "One Argo CD project per layer, default project empty; Kyverno checks every object"),
            ("deliver", "Drift reverted", "Manual kubectl changes are undone; make gitops-status shows sync and health"),
        ],
        "stats": [
            ("6 / 6", "applications Synced and Healthy after bootstrap, data intact", "deliver"),
            ("1 chart set", "for kind and EKS: only the values differ", "process"),
            ("< 1 min", "from push to sync", "observe"),
        ],
        "source": "gitops/README.md, helm/README.md, docs/architecture-decisions/0006-helm-and-gitops.md",
    },
    {
        "name": "aws-lifecycle",
        "title": "AWS environment lifecycle",
        "subtitle": "Ephemeral by design: apply, verify end to end, destroy, and prove nothing billable is left",
        "steps": [
            ("aws", "aws-init, aws-plan", "Remote state in S3 with a lockfile; the saved plan is reviewed"),
            ("people", "Human confirms", "Every apply is confirmed by a person, then exactly the saved plan is applied"),
            ("aws", "aws-apply", "VPC over 3 AZs, EKS, IRSA, KMS, S3, Glue, Athena, RDS, ECR, AgentCore Gateway"),
            ("deliver", "aws-up", "Security gate, signed images to ECR, then the same GitOps bootstrap as kind"),
            ("data", "aws-smoke", "Events through the real pipeline, counted with Athena"),
            ("secure", "aws-security-test", "Signatures, External Secrets, RDS TLS, audit services, then the Kafka, network and admission suites"),
            ("ai", "aws-agentcore, aws-ai-test", "AgentCore Gateway to the MCP server in the VPC; Runtime selftest; an alert end to end"),
            ("aws", "aws-down", "Kubernetes volumes and load balancers first, terraform destroy, then a leftover check"),
        ],
        "stats": [
            ("~$0.40 / h", "while the environment is up", "aws"),
            ("5,000 = 5,000", "events in, rows counted by Athena", "data"),
            ("10/10 · 9/9 · 15/15", "Kafka, network and admission tests on EKS", "secure"),
            ("21 min", "teardown, leftover check clean (2026-10-29)", "deliver"),
        ],
        "note": "2026-11-01: AgentCore Runtime network interfaces held the VPC after the Runtime was deleted; aws-down now waits for them and retries.",
        "source": "docs/aws.md, docs/ai-ops.md",
    },
    {
        "name": "supply-chain",
        "title": "Software supply chain",
        "subtitle": "From source to a running pod, every platform image is locked, scanned, described and signed",
        "steps": [
            ("deliver", "Build", "Multi-stage Dockerfiles, pinned base images; Python dependencies hash-locked, releases at least 14 days old"),
            ("secure", "Trivy gate", "Fails on fixable HIGH or CRITICAL findings; also scans Terraform and Helm"),
            ("secure", "SBOM", "CycloneDX bill of materials for each image"),
            ("aws", "Push to ECR", "KMS-encrypted repositories, scan on push"),
            ("secure", "Sign by digest", "Cosign with a KMS key: the key never leaves AWS"),
            ("secure", "Verify at admission", "Kyverno on EKS admits only images signed by that key"),
        ],
        "stats": [
            ("7 + 1", "platform images signed, plus the agent's arm64 build for AgentCore", "aws"),
            ("admitted / refused", "signed image runs, unsigned image is rejected", "secure"),
            ("3 / 3", "Python images pass the gate after base and dependency fixes", "deliver"),
        ],
        "source": "SECURITY.md, local/scripts/security-scan.sh, local/scripts/aws-images.sh",
    },
    {
        "name": "kafka-mtls",
        "title": "Kafka mutual TLS and certificate distribution",
        "subtitle": "No passwords: every client proves who it is with a certificate, and can do only what its ACLs allow",
        "steps": [
            ("secure", "Two CAs", "Strimzi runs a cluster CA (brokers) and a clients CA (users), and renews them"),
            ("secure", "One KafkaUser per client", "Certificate with CN=<name>; least-privilege ACLs"),
            ("secure", "Distribute", "A Kyverno generate policy clones labelled secrets into the client namespace and keeps them in sync"),
            ("data", "Clients connect", "ingestion-service: Write events.raw. Flink job: Read events.raw and its group"),
            ("secure", "One listener", "Port 9093, TLS with client authentication only; a network policy admits only the apps namespace"),
        ],
        "stats": [
            ("10/10", "Kafka security tests on EKS: allowed and refused per identity", "secure"),
            ("0", "plaintext or unauthenticated listeners", "data"),
            ("3", "identities: ingestion, Flink job, admin toolbox", "deliver"),
        ],
        "note": "Not yet tested: a real CA renewal end to end (the copy and sync were tested by deleting and tampering with a copied secret).",
        "source": "SECURITY.md, helm/charts/kafka-cluster/values.yaml",
    },
    {
        "name": "agentic-ops",
        "title": "Agentic ops loop",
        "subtitle": "An alert gets a diagnosis with evidence in minutes; anything that changes the platform needs a person",
        "steps": [
            ("observe", "Alert fires", "PrometheusRule to Alertmanager, which calls the agent's webhook"),
            ("ai", "Gate", "De-duplicate and rate-limit before any model call"),
            ("ai", "Investigate", "Pydantic AI agent with Claude Sonnet 4.6 on Bedrock calls read-only MCP tools: metrics, logs, pods, Kafka, Flink, Iceberg, Argo CD, runbooks"),
            ("ai", "Diagnosis", "Typed Pydantic output: category, evidence, confidence, one action from a closed catalogue"),
            ("secure", "Validate", "Checked against the catalogue twice; parameters hashed"),
            ("people", "Slack approval", "Block Kit message; Approve is bound to that exact hash"),
            ("secure", "Single-use token", "HMAC-signed, 60 s, compare-and-set: runs once at most"),
            ("deliver", "Execute and audit", "Only ops-control has write RBAC; every step is stored in Postgres; traces in Langfuse"),
        ],
        "stats": [
            ("13", "read-only MCP tools, shared with the Hermes on-call chat", "ai"),
            ("5", "catalogue actions (none, escalate, 3 restarts or compaction)", "secure"),
            ("37 / 37", "guardrail checks on kind", "deliver"),
            ("1.38 s", "tool call AgentCore Runtime → Gateway → EKS", "aws"),
        ],
        "note": "Model calls are not verified yet: the AWS account has no Bedrock access to Claude models, so the eval suite has no results.",
        "source": "docs/ai-ops.md, docs/architecture-decisions/0010-agentic-ops.md",
    },
]

W = 1280
MARGIN = 40
GAP = 44
FONT = "'Segoe UI', 'Helvetica Neue', Helvetica, Arial, sans-serif"
INK, MUTED, LINE = "#0f172a", "#475569", "#cbd5e1"


def wrap(text, size, width):
    """Greedy word wrap with an average glyph width of 0.5 em."""
    limit = max(8, int(width / (size * 0.5)))
    lines, line = [], ""
    for word in text.split():
        if line and len(line) + 1 + len(word) > limit:
            lines.append(line)
            line = word
        else:
            line = f"{line} {word}".strip()
    return lines + ([line] if line else [])


def text(x, y, s, size, colour=INK, weight="normal", anchor="start"):
    return (f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{colour}" '
            f'text-anchor="{anchor}">{escape(s)}</text>')


def render(flow):
    out, used = [], []
    steps = flow["steps"]
    for layer, _, _ in steps:
        if layer not in used:
            used.append(layer)
    for _, _, layer in flow["stats"]:
        if layer not in used:
            used.append(layer)

    y = 0
    out.append(f'<rect x="0" y="0" width="{W}" height="118" fill="#0f172a"/>')
    out.append(text(MARGIN, 52, flow["title"], 32, "#ffffff", "bold"))
    out.append(text(MARGIN, 86, flow["subtitle"], 16, "#cbd5e1"))
    lx = W - MARGIN
    for layer in reversed(used):
        label, colour = LAYERS[layer]
        w = int(len(label) * 7.2) + 30
        lx -= w
        out.append(f'<rect x="{lx}" y="20" width="{w - 8}" height="24" rx="12" fill="{colour}"/>')
        out.append(text(lx + (w - 8) / 2, 37, label, 12, "#ffffff", "bold", "middle"))
    y = 150

    # Step cards, 3 or 4 per row; an elbow connector joins the rows.
    cols = min(4, max(3, (len(steps) + 1) // 2))
    card_w = (W - 2 * MARGIN - (cols - 1) * GAP) // cols
    rows = [steps[i:i + cols] for i in range(0, len(steps), cols)]
    for r, row in enumerate(rows):
        bodies = [wrap(detail, 14, card_w - 32) for _, _, detail in row]
        titles = [wrap(title, 18, card_w - 70) for _, title, _ in row]
        h = 70 + max(len(t) for t in titles) * 22 + max(len(b) for b in bodies) * 19 + 16
        for c, (layer, _, _) in enumerate(row):
            n = r * cols + c + 1
            colour = LAYERS[layer][1]
            x = MARGIN + c * (card_w + GAP)
            out.append(f'<rect x="{x}" y="{y}" width="{card_w}" height="{h}" rx="14" fill="{colour}0f" '
                       f'stroke="{colour}" stroke-width="2"/>')
            out.append(f'<rect x="{x}" y="{y}" width="{card_w}" height="8" rx="4" fill="{colour}"/>')
            out.append(f'<circle cx="{x + 30}" cy="{y + 40}" r="19" fill="{colour}"/>')
            out.append(text(x + 30, y + 47, str(n), 19, "#ffffff", "bold", "middle"))
            ty = y + 46 - (len(titles[c]) - 1) * 11
            for line in titles[c]:
                out.append(text(x + 60, ty, line, 18, INK, "bold"))
                ty += 22
            by = y + 70 + (len(titles[c]) - 1) * 22 + 8
            for line in bodies[c]:
                out.append(text(x + 18, by, line, 14, MUTED))
                by += 19
            if c < len(row) - 1:                      # arrow to the next card in the row
                ax, ay = x + card_w + 6, y + h / 2
                out.append(f'<path d="M{ax} {ay} h{GAP - 18}" stroke="{LINE}" stroke-width="4" '
                           f'stroke-linecap="round"/>')
                out.append(f'<path d="M{ax + GAP - 16} {ay - 8} l10 8 l-10 8 z" fill="#94a3b8"/>')
        if r < len(rows) - 1:                         # elbow: end of this row to the start of the next
            ex = MARGIN + (len(row) - 1) * (card_w + GAP) + card_w / 2
            sx = MARGIN + card_w / 2
            y1, y2 = y + h + 4, y + h + GAP - 6
            mid = y + h + GAP / 2
            out.append(f'<path d="M{ex} {y1} V{mid} H{sx} V{y2 - 10}" fill="none" stroke="{LINE}" '
                       f'stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>')
            out.append(f'<path d="M{sx - 8} {y2 - 12} l8 10 l8 -10 z" fill="#94a3b8"/>')
        y += h + GAP

    stats = flow["stats"]
    sw = (W - 2 * MARGIN - (len(stats) - 1) * 20) / len(stats)
    out.append(text(MARGIN, y + 6, "MEASURED", 13, MUTED, "bold"))
    y += 22
    sh = 118
    for i, (value, label, layer) in enumerate(stats):
        colour = LAYERS[layer][1]
        x = MARGIN + i * (sw + 20)
        out.append(f'<rect x="{x}" y="{y}" width="{sw}" height="{sh}" rx="14" fill="#ffffff" '
                   f'stroke="{LINE}" stroke-width="1.5"/>')
        out.append(f'<rect x="{x}" y="{y + 14}" width="6" height="{sh - 28}" rx="3" fill="{colour}"/>')
        size = 34 if len(value) <= 12 else 26
        out.append(text(x + 24, y + 50, value, size, colour, "bold"))
        ly = y + 76
        for line in wrap(label, 13, sw - 40)[:3]:
            out.append(text(x + 24, ly, line, 13, MUTED))
            ly += 17
    y += sh + 22

    if flow.get("note"):
        lines = wrap(flow["note"], 14, W - 2 * MARGIN - 60)
        nh = 22 + len(lines) * 19
        out.append(f'<rect x="{MARGIN}" y="{y}" width="{W - 2 * MARGIN}" height="{nh}" rx="10" '
                   f'fill="#fef9c3" stroke="#ca8a04" stroke-width="1.5" stroke-dasharray="6 4"/>')
        out.append(text(MARGIN + 18, y + 27, "!", 18, "#a16207", "bold"))
        ly = y + 26
        for line in lines:
            out.append(text(MARGIN + 40, ly, line, 14, "#713f12"))
            ly += 19
        y += nh + 18

    out.append(text(MARGIN, y + 8, f"Details: {flow['source']}   ·   regenerate with make diagrams", 12, "#94a3b8"))
    height = y + 30
    head = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{height}" viewBox="0 0 {W} {height}" '
            f'font-family="{escape(FONT, {chr(39): "&apos;"})}" role="img" aria-label="{escape(flow["title"])}">'
            f'<title>{escape(flow["title"])}</title>'
            f'<rect width="{W}" height="{height}" rx="18" fill="#f8fafc"/>'
            f'<clipPath id="clip-{flow["name"]}"><rect width="{W}" height="{height}" rx="18"/></clipPath><g clip-path="url(#clip-{flow["name"]})">')
    return head + "".join(out) + "</g></svg>\n"


def main():
    out_dir = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "docs/diagrams")
    os.makedirs(out_dir, exist_ok=True)
    for flow in WORKFLOWS:
        path = os.path.join(out_dir, f"workflow-{flow['name']}.svg")
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(render(flow))
    print(f"wrote {len(WORKFLOWS)} workflow infographics to {out_dir}")


if __name__ == "__main__":
    main()
