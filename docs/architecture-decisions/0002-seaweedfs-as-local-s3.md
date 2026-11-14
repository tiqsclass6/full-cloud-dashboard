# **ADR 0002: SeaweedFS as the local S3-compatible store**

- Status: accepted
- Date: 2026-10-28

## **Context**
The local platform needs an S3 API for Iceberg tables, Flink checkpoints and Spark event logs. MinIO was the usual choice, but its community repository was archived in April 2026 and its container images are no longer maintained upstream.

## **Decision**
Use **SeaweedFS** (Apache 2.0, `chrislusf/seaweedfs`) in single-node `server -s3` mode, with path-style addressing and a static dev credential. Buckets (`warehouse`, `checkpoints`, `spark-events`) are created by an idempotent Job using the AWS CLI, which doubles as an S3 API check.

## **Alternatives considered**
- Garage: lightweight, but needs a cluster-layout bootstrap step.
- RustFS: young project.
- Ceph RGW: too heavy for a laptop.
- LocalStack: not appropriate for an Iceberg and Flink data path.

## **Consequences**
- It is a stand-in only: production semantics (S3 consistency, SSE-KMS, bucket policies, lifecycle rules) are validated on real S3 during the AWS demo.
- Any S3-compat quirk found with Iceberg, Flink or Spark is recorded here. Applications use standard S3 client settings, so the swap to AWS is configuration only.
