SHELL := bash
SCRIPTS := local/scripts

.PHONY: help local-up local-down local-smoke local-ports local-status local-load local-deploy-ingest local-loadtest local-perf local-deploy-flink local-test-exactly-once local-demo-schema-evolution local-test-recovery local-test-integrity local-spark-reconcile local-replay local-repair-duplicates local-deploy-spark local-spark-test local-spark-maintenance local-spark-aggregate local-spark-schedules gitops-push gitops-bootstrap gitops-status helm-check docs-format docs-check tf-fmt tf-validate aws-init aws-plan aws-apply aws-up aws-gitops-push aws-smoke aws-down security-scan security-test aws-security-test rules-test dashboards diagrams chaos ai-test ai-deploy ai-creds ai-ops-test ai-eval ai-lock langfuse-on langfuse-off aws-agentcore aws-ai-test

help:
	@echo "Local platform (kind):"
	@echo "  make local-up      create the cluster and bring the platform up through GitOps"
	@echo "  make local-smoke   run end-to-end checks"
	@echo "  make local-ports   port-forward Argo CD/Gitea/Grafana/Prometheus/Apicurio/Iceberg/S3"
	@echo "  make local-status  show pods across platform namespaces"
	@echo "  make local-down    delete the cluster"
	@echo "Ingestion service (needs the cluster):"
	@echo "  make local-deploy-ingest            build + deploy"
	@echo "  make local-loadtest RATE=10000      load test (RATE events/s, DURATION s)"
	@echo "  make local-perf LABEL=x GC=g1|zgc   JFR-profiled run, writes docs/performance/results"
	@echo "Flink -> Iceberg pipeline (needs the cluster):"
	@echo "  make local-deploy-flink             build + deploy the Flink job"
	@echo "  make local-test-exactly-once        destructive: resets topic+table, injects failures, verifies"
	@echo "  make local-demo-schema-evolution    add a column end to end"
	@echo "  make local-test-recovery            catalog outage + JobManager loss; verifies no loss or duplicates"
	@echo "  make local-test-integrity           undecodable records kept + alerted; a deleted row found by reconcile + alerted"
	@echo "  make local-repair-duplicates        runbook: suspend job, dedupe + compact, resume, verify"
	@echo "Spark jobs (needs the cluster and Flink data):"
	@echo "  make local-spark-test               pytest suite inside the Spark image (no cluster needed)"
	@echo "  make local-deploy-spark             build the Spark image and load it into kind"
	@echo "  make local-spark-maintenance        compact + manifests + expire + orphans, with stats"
	@echo "  make local-spark-aggregate          hourly aggregates (events_hourly)"
	@echo "  make local-load                     memory and CPU per pod and namespace (from Prometheus; kubectl top does not work here)"
	@echo "  make local-spark-reconcile          offset reconciliation of the last 24 h (events_reconciliation)"
	@echo "  make local-replay ARGS='missing --partition 3 --offset 276204'   re-send lost/undecodable records (dry run; add --apply)"
	@echo "  make local-spark-schedules          show the scheduled jobs (defined in helm/charts/spark-jobs)"
	@echo "GitOps and Helm:"
	@echo "  make gitops-push                    publish the working tree to the in-cluster Gitea (Argo CD syncs it)"
	@echo "  make gitops-status                  Argo CD applications: sync and health"
	@echo "  make gitops-bootstrap               (re)install Argo CD + Gitea and the root application"
	@echo "  make helm-check                     lint, render and schema-check the charts for every environment"
	@echo "Security (Phase 7):"
	@echo "  make security-scan                  Trivy: image vulnerabilities (gated), SBOMs, IaC misconfigurations"
	@echo "  make security-test                  Kafka mTLS/ACLs, network policies, admission policies (local cluster)"
	@echo "Observability and chaos (Phase 8):"
	@echo "  make rules-test                     promtool check + unit tests for the alert rules"
	@echo "  make dashboards                     regenerate the Grafana dashboards (local/tools/dashboards.py)"
	@echo "  make chaos EXP=broker-isolation     run one chaos experiment (chaos/README.md)"
	@echo "Ops agent (Phase 9, docs/ai-ops.md):"
	@echo "  make ai-test                        unit tests for the MCP tools and the agent (in Docker, no model calls)"
	@echo "  make ai-deploy                      build the images, create the secrets, roll out (kind)"
	@echo "  make ai-creds                       1-hour model credentials for the agent on kind (assumes the bootstrap role)"
	@echo "  make ai-ops-test                    guardrails: RBAC, tokens, network, approval path (no model needed)"
	@echo "  make ai-eval RUNS=1                 replay the incident scenarios through the model and score them (costs tokens)"
	@echo "  make ai-lock [ARGS=...]             re-lock the Python dependencies (releases at least two weeks old); ARGS='--upgrade-package X' bumps X"
	@echo "  make langfuse-on / langfuse-off     LLM tracing for the agent on kind (off by default, about 2 GiB)"
	@echo "Docs:"
	@echo "  make docs-format / docs-check       align Markdown tables (check = report only)"
	@echo "  make diagrams                       light reference diagrams in build/diagrams (docs/diagrams holds the dark set)"
	@echo "Terraform:"
	@echo "  make tf-fmt tf-validate"
	@echo "AWS (ephemeral EKS environment; ENV=dev by default; see docs/aws.md):"
	@echo "  make aws-init / aws-plan / aws-apply   Terraform for terraform/envs/$$ENV (apply uses the saved plan)"
	@echo "  make aws-up                            images to ECR, secrets, GitOps bootstrap on EKS"
	@echo "  make aws-gitops-push                   publish the working tree to the EKS cluster's Gitea (gitops-push targets kind)"
	@echo "  make aws-smoke                         end-to-end check: events -> Flink -> Iceberg (Glue) -> Athena"
	@echo "  make aws-security-test                 image signatures, External Secrets, RDS TLS, audit services, cluster suites"
	@echo "  make aws-down                          delete volumes through Kubernetes, terraform destroy, leftover check"

local-up:
	bash $(SCRIPTS)/up.sh

local-smoke:
	bash $(SCRIPTS)/smoke.sh

local-ports:
	bash $(SCRIPTS)/ports.sh

local-status:
	@kubectl --context kind-lakehouse get pods -A -o wide | grep -E "NAME|^(data|kafka|monitoring|apps|flink-operator|spark-operator|argocd|gitops) "

RATE ?= 5000
DURATION ?= 60
LABEL ?= run
GC ?= g1

local-deploy-ingest:
	bash $(SCRIPTS)/deploy-ingestion.sh

local-loadtest:
	bash $(SCRIPTS)/loadtest.sh $(RATE) $(DURATION) 10 50 64

local-perf:
	bash $(SCRIPTS)/perf-run.sh $(LABEL) $(GC) 30000 70 50

local-deploy-flink:
	bash $(SCRIPTS)/deploy-flink-job.sh

local-test-exactly-once:
	bash $(SCRIPTS)/flink-eo-test.sh

local-demo-schema-evolution:
	bash $(SCRIPTS)/flink-schema-evolution-demo.sh

local-spark-test:
	bash apps/spark-jobs/run-tests.sh

local-test-recovery:
	bash $(SCRIPTS)/flink-recovery-test.sh

local-test-integrity:
	bash $(SCRIPTS)/integrity-drill.sh

local-repair-duplicates:
	bash $(SCRIPTS)/repair-duplicates.sh

local-deploy-spark:
	bash $(SCRIPTS)/deploy-spark-jobs.sh

local-spark-maintenance:
	bash $(SCRIPTS)/spark-run.sh events-maintenance maintenance --benchmark-query

local-spark-aggregate:
	bash $(SCRIPTS)/spark-run.sh events-aggregate aggregate --lookback-hours 24

local-load:
	bash $(SCRIPTS)/cluster-load.sh

local-spark-reconcile:
	bash $(SCRIPTS)/spark-run.sh events-reconcile reconcile --lookback-hours 24 --settle-minutes 0

local-replay:
	bash $(SCRIPTS)/replay.sh $(ARGS)

local-spark-schedules:
	kubectl --context kind-lakehouse -n apps get scheduledsparkapplications

gitops-push:
	bash $(SCRIPTS)/gitops-push.sh

gitops-bootstrap:
	bash $(SCRIPTS)/gitops-bootstrap.sh

gitops-status:
	@kubectl --context kind-lakehouse -n argocd get applications -o custom-columns=APP:.metadata.name,SYNC:.status.sync.status,HEALTH:.status.health.status,REVISION:.status.sync.revision

helm-check:
	bash $(SCRIPTS)/helm-check.sh

local-down:
	bash $(SCRIPTS)/down.sh

docs-format:
	python local/tools/format_md_tables.py

docs-check:
	python local/tools/format_md_tables.py --check

tf-fmt:
	terraform fmt -recursive terraform

tf-validate:
	cd terraform/bootstrap && terraform init -backend=false -input=false && terraform validate
	cd terraform/envs/dev && terraform init -backend=false -input=false && terraform validate

ENV ?= dev
TF_ENV := terraform -chdir=terraform/envs/$(ENV)

# The state bucket name contains the account id, so it is derived at run time rather than stored.
aws-init:
	$(TF_ENV) init -input=false -backend-config="bucket=lakehouse-tfstate-$$(aws sts get-caller-identity --query Account --output text)"

aws-plan:
	$(TF_ENV) plan -input=false -var-file=terraform.tfvars -out=tfplan

aws-apply:
	$(TF_ENV) apply -input=false tfplan

aws-up:
	bash $(SCRIPTS)/aws-up.sh $(ENV)

aws-gitops-push:
	KUBE_CONTEXT=lakehouse-$(ENV) GITOPS_ENV=$(ENV) bash $(SCRIPTS)/gitops-push.sh

aws-smoke:
	bash $(SCRIPTS)/aws-smoke.sh $(ENV)

aws-down:
	bash $(SCRIPTS)/aws-down.sh $(ENV)

security-scan:
	bash $(SCRIPTS)/security-scan.sh

security-test:
	bash $(SCRIPTS)/kafka-security-test.sh
	bash $(SCRIPTS)/network-policy-test.sh
	bash $(SCRIPTS)/policy-test.sh

aws-security-test:
	bash $(SCRIPTS)/aws-security-test.sh $(ENV)

rules-test:
	bash $(SCRIPTS)/rules-test.sh

diagrams:
	bash $(SCRIPTS)/diagrams.sh

dashboards:
	python local/tools/dashboards.py

EXP ?= broker-isolation
chaos:
	bash $(SCRIPTS)/chaos-run.sh $(EXP)

ai-test:
	bash apps/mcp-platform-tools/run-tests.sh
	bash apps/ai-ops/run-tests.sh

ai-deploy:
	bash $(SCRIPTS)/deploy-ai-ops.sh

ai-creds:
	bash $(SCRIPTS)/ai-creds.sh

ai-ops-test:
	bash $(SCRIPTS)/ai-ops-test.sh

RUNS ?= 1
ai-eval:
	bash $(SCRIPTS)/ai-eval.sh $(RUNS)

ai-lock:
	bash $(SCRIPTS)/ai-lock.sh $(ARGS)

langfuse-on:
	bash $(SCRIPTS)/langfuse.sh on

langfuse-off:
	bash $(SCRIPTS)/langfuse.sh off

aws-agentcore:
	bash $(SCRIPTS)/aws-agentcore.sh $(ENV) up

aws-ai-test:
	bash $(SCRIPTS)/aws-ai-test.sh $(ENV)
