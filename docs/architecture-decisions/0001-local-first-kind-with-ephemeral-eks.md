# **ADR 0001: Local-first development on kind, ephemeral EKS on AWS**

- Status: accepted
- Date: 2026-10-28

## **Context**
The platform (Kafka, Flink, Spark, Iceberg, observability, an AI ops agent) is expensive to run on AWS around the clock. The monthly budget is $100, and an idle EKS control plane alone costs about $73/month.

## **Decision**
- Develop and test everything on a local **kind** cluster with 1 control-plane and 3 workers. Each worker is labelled with a distinct `topology.kubernetes.io/zone`, so rack-aware Kafka and topology spread behave like a 3-AZ cluster.
- Use the same Helm charts and values layering locally and on AWS; only the overlay differs.
- Provision **EKS in us-east-1 with Terraform only for demos**: apply, demo, destroy. A budget with forecast alerts (90% and 95%) is applied first and lives in a separate stack so it survives teardown.

## **Consequences**
- Fast, free inner loop; AWS spend is limited to short demo windows.
- Some components differ between environments and sit behind an interface: the S3 stand-in (ADR 0002), the Iceberg catalog (REST/JDBC locally, Glue on AWS), and the query engine (Trino locally, Athena on AWS).
- AWS-only services (Bedrock AgentCore, Athena, Glue) cannot be exercised locally; the agent runs as a plain container on kind.
