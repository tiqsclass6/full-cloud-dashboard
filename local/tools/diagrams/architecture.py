#!/usr/bin/env python3
"""Architecture diagrams with official icons (Python `diagrams` library, rendered by Graphviz).

  architecture.py OUT_DIR      writes architecture.png (logical platform) and aws-deployment.png
"""
import os
import sys

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import Athena, GlueDataCatalog
from diagrams.aws.compute import ECR, EKS
from diagrams.aws.cost import Budgets
from diagrams.aws.database import RDSPostgresqlInstance
from diagrams.aws.management import Cloudtrail, Config
from diagrams.aws.ml import Bedrock
from diagrams.aws.network import NLB, Privatelink
from diagrams.aws.security import IAMRole, KMS, Guardduty, SecretsManager
from diagrams.aws.storage import S3
from diagrams.generic.storage import Storage
from diagrams.k8s.chaos import ChaosMesh
from diagrams.k8s.controlplane import API
from diagrams.k8s.network import NetworkPolicy
from diagrams.k8s.podconfig import Secret
from diagrams.onprem.analytics import Flink, Spark
from diagrams.onprem.client import Users
from diagrams.onprem.database import ClickHouse, PostgreSQL
from diagrams.onprem.gitops import ArgoCD
from diagrams.onprem.logging import Loki
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.queue import Kafka
from diagrams.onprem.tracing import Tempo
from diagrams.onprem.vcs import Gitea
from diagrams.programming.language import Java, Python
from diagrams.saas.chat import Slack

# One colour per layer, shared with the infographics.
DATA, PROCESS, OBSERVE, DELIVER, SECURE, AI, AWS = (
    "#2563eb", "#0891b2", "#ea580c", "#16a34a", "#dc2626", "#7c3aed", "#d97706")

GRAPH = {"fontsize": "26", "fontname": "Helvetica-Bold", "pad": "0.6", "nodesep": "0.55", "ranksep": "1.0",
         "splines": "spline", "labelloc": "t", "bgcolor": "white"}
NODE = {"fontsize": "12", "fontname": "Helvetica"}


def cluster(label, colour):
    return Cluster(label, graph_attr={"bgcolor": colour + "12", "pencolor": colour, "penwidth": "2",
                                      "fontcolor": colour, "fontsize": "16", "fontname": "Helvetica-Bold",
                                      "style": "rounded", "margin": "18"})


def edge(colour, label="", style="solid", layout=True):
    """layout=False: drawn, but does not move nodes (keeps the main left-to-right path straight)."""
    return Edge(color=colour, label=label, style=style, penwidth="2", fontcolor=colour, fontsize="11",
                fontname="Helvetica", constraint="true" if layout else "false")


def platform(out_dir):
    with Diagram("Streaming lakehouse platform: kind locally, Amazon EKS on AWS", show=False, direction="LR",
                 filename=os.path.join(out_dir, "architecture"), outformat="png", graph_attr=GRAPH, node_attr=NODE):
        clients = Users("clients and\nload generator")

        with cluster("Kubernetes  (kind: 4 nodes, 3 zones  |  EKS: 3 AZs)", "#334155"):
            with cluster("Ingest", DATA):
                ingest = Java("ingestion-service\nJava 21, Avro, Micrometer")
                registry = Storage("Apicurio\nschema registry")
            with cluster("Stream  (Strimzi, KRaft)", DATA):
                kafka = Kafka("Kafka 4.3, 3 brokers\nRF=3, min ISR=2, mTLS")
            with cluster("Process", PROCESS):
                flink = Flink("Flink 2.1 job\nexactly once, 10 s checkpoints")
                spark = Spark("Spark 4.1 (PySpark)\ncompaction, expiry, aggregates")
            with cluster("Lakehouse", DATA):
                iceberg = Storage("Apache Iceberg tables\n(Parquet)")
                catalog = PostgreSQL("Iceberg REST catalog\n(JDBC on Postgres)  |  Glue on AWS")
                objects = Storage("object storage\nSeaweedFS  |  Amazon S3 (KMS)")
            with cluster("Observe", OBSERVE):
                prom = Prometheus("Prometheus + Alertmanager\nscrapes every component\nSLO burn-rate alerts")
                grafana = Grafana("Grafana\ndashboards as code")
                loki = Loki("Loki logs")
                tempo = Tempo("Tempo traces")
                exporter = Java("iceberg-exporter\ntable health")
            with cluster("Agentic ops", AI):
                agent = Python("ops-agent\nPydantic AI")
                tools = Python("mcp-platform-tools\n13 read-only MCP tools")
                control = Python("ops-control\napprovals, executor, audit")
                hermes = Python("Hermes Agent\non-call chat (read-only)")
                langfuse = ClickHouse("Langfuse\nLLM traces (local)")
            with cluster("Deliver and govern", DELIVER):
                gitea = Gitea("Gitea\nin-cluster Git")
                argo = ArgoCD("Argo CD\ndeploys every chart from Git")
                kyverno = API("Kyverno\nadmission policies\nsigned images (EKS)")
                netpol = NetworkPolicy("network policies\ndefault deny")
                chaos = ChaosMesh("Chaos Mesh")

        model = Bedrock("Claude Sonnet 4.6\non Amazon Bedrock")
        slack = Slack("Slack\napprove / reject")
        athena = Athena("Amazon Athena\n(AWS query)")

        clients >> edge(DATA, "HTTP JSON") >> ingest
        ingest >> edge(DATA, "Avro, idempotent") >> kafka
        ingest >> edge(DATA, "schemas", "dashed", False) >> registry
        kafka >> edge(DATA, "events.raw") >> flink
        flink >> edge(DATA, "2-phase commit") >> iceberg
        spark >> edge(PROCESS, "rewrite, expire") >> iceberg
        iceberg >> edge(DATA) >> catalog
        iceberg >> edge(DATA) >> objects
        athena >> edge(AWS, "SQL", "dashed", False) >> iceberg

        exporter >> edge(OBSERVE, "scrape", "dotted") >> prom
        prom >> edge(OBSERVE) >> grafana

        prom >> edge(AI, "alert webhook") >> agent
        agent >> edge(AI, "MCP") >> tools
        hermes >> edge(AI, "MCP") >> tools
        tools >> edge(AI, "PromQL, LogQL, K8s API", "dashed", False) >> [prom, loki]
        agent >> edge(AI, "Claude") >> model
        agent >> edge(AI, "proposal") >> control
        control >> edge(AI, "Block Kit") >> slack
        agent >> edge(AI, "traces", "dotted") >> langfuse

        gitea >> edge(DELIVER, "sync") >> argo
        gitea >> edge(DELIVER, style="invis") >> kyverno


def aws(out_dir):
    with Diagram("AWS dev environment (Terraform, ephemeral: apply, verify, destroy)", show=False, direction="LR",
                 filename=os.path.join(out_dir, "aws-deployment"), outformat="png", graph_attr=GRAPH, node_attr=NODE):
        with cluster("bootstrap stack (stays)", SECURE):
            budget = Budgets("Budget $100/month\nalerts 90% / 95% / 100%")
            trail = Cloudtrail("CloudTrail")
            config = Config("AWS Config\nrules")
            guard = Guardduty("GuardDuty high findings\nto SNS")
            state = S3("audit bucket (KMS)\n+ Terraform state bucket")

        with cluster("VPC  (3 AZs, private subnets, 1 NAT)", AWS):
            with cluster("Amazon EKS", "#334155"):
                eks = EKS("EKS 1.37\nmanaged nodes")
                workloads = Kafka("Kafka, Flink, Spark\nobservability, ai-ops")
                mcp = Python("mcp-platform-tools\nTLS :8443")
            nlb = NLB("internal NLB\n(no node ports)")
            rds = RDSPostgresqlInstance("RDS Postgres\nmetadata, agent audit")
            lattice = Privatelink("VPC Lattice\nresource")

        with cluster("Regional services", DATA):
            s3 = S3("S3 lakehouse\nwarehouse, checkpoints")
            glue = GlueDataCatalog("Glue Data Catalog\nIceberg tables")
            athena = Athena("Athena\nworkgroup")

        with cluster("Security", SECURE):
            kms = KMS("KMS key\nS3, EBS, secrets, signing")
            irsa = IAMRole("IRSA roles\nper workload")
            secrets = SecretsManager("Secrets Manager\n(External Secrets)")
            ecr = ECR("ECR\nsigned images")

        with cluster("Amazon Bedrock AgentCore", AI):
            runtime = Bedrock("AgentCore Runtime\nops-agent (arm64)")
            gateway = Bedrock("AgentCore Gateway\nMCP, SigV4 inbound")
            model = Bedrock("Claude Sonnet 4.6\n(access pending)")

        eks >> edge("#334155") >> workloads
        workloads >> edge(DATA, "IRSA") >> [s3, glue]
        athena >> edge(DATA, "SQL") >> glue
        glue >> edge(DATA) >> s3
        workloads >> edge(DATA, "TLS") >> rds
        irsa >> edge(SECURE, style="dashed") >> workloads
        secrets >> edge(SECURE, style="dashed") >> workloads
        kms >> edge(SECURE, style="dotted") >> [s3, rds, secrets]
        ecr >> edge(SECURE, "Cosign verified at admission", "dashed") >> eks

        runtime >> edge(AI, "MCP") >> gateway
        gateway >> edge(AI, "private CA, bearer") >> lattice
        lattice >> edge(AI) >> nlb
        nlb >> edge(AI) >> mcp
        runtime >> edge(AI, "Messages API") >> model
        [trail, config] >> edge(SECURE, style="dotted") >> state


def main():
    out_dir = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "docs/diagrams")
    os.makedirs(out_dir, exist_ok=True)
    platform(out_dir)
    aws(out_dir)
    print(f"wrote {out_dir}/architecture.png and aws-deployment.png")


if __name__ == "__main__":
    main()
