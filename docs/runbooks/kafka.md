# **Runbook: Kafka**

Alerts from `helm/charts/observability/rules/kafka.yaml`. Dashboard: **Lakehouse / Kafka**. Topics use RF=3, `min.insync.replicas=2` and `acks=all`: one broker can be lost without data loss or downtime; a second loss stops writes.

Admin commands run in the admin toolbox (mutual TLS as `kafka-admin`): `source local/scripts/lib.sh`, then `kafka_cli <tool.sh> ...`.

## **KafkaBrokerDown**

**Meaning:** fewer than 3 brokers are reachable for 2 minutes.
**Impact:** none yet for clients (RF=3, min ISR 2), but there is no margin left: one more loss stops writes.
**Diagnose:** `kc -n kafka get pods -o wide` (which broker, which node); `kc -n kafka describe pod <broker>`; broker logs in Loki: `{namespace="kafka", pod="<broker>"}`. On a node failure, the pod cannot move to another zone (its volume is zonal); on EKS the per-zone node group replaces the node. A broker that is Running and Ready but still not reachable (its metrics endpoint not scraped, its replicas out of the ISR, the other brokers logging connection timeouts to it) is cut off from the network, not crashed: check NetworkPolicies, the node's network and recent changes (including chaos experiments).
**Fix:** Strimzi restarts a crashed broker. If the pod is Pending, check the node and its volume. Do not delete the PVC: the broker would rejoin empty and have to re-replicate everything. Do not restart a broker that is cut off from the network: the restart does not reach the network fault, and it removes the evidence (its connection errors and ISR history). Escalate to whoever owns the network; Kafka keeps serving with two brokers.

## **KafkaUnderReplicatedPartitions**

**Meaning:** some replicas are behind their leader for 5 minutes.
**Impact:** reduced durability margin while it lasts.
**Diagnose:** usually follows a broker restart (replicas catching up) or a slow disk. `kafka_cli kafka-topics.sh --describe --under-replicated-partitions`.
**Fix:** wait for catch-up after a restart; persistent lag on one broker points to its disk or node.

## **KafkaUnderMinIsr**

**Meaning:** some partitions have fewer than 2 in-sync replicas.
**Impact:** producers with `acks=all` (the ingestion service) get errors for those partitions: ingest failures, error budget burn.
**Diagnose:** `kafka_cli kafka-topics.sh --describe --under-min-isr-partitions`; which brokers are down (KafkaBrokerDown).
**Fix:** bring brokers back. Never lower `min.insync.replicas` to make writes pass: that trades the outage for silent data loss.

## **KafkaOfflinePartitions**

**Meaning:** partitions without a leader.
**Impact:** those partitions are neither readable nor writable.
**Fix:** as KafkaUnderMinIsr; unclean leader election is disabled on purpose and must stay disabled.

## **KafkaNoActiveController**

**Meaning:** the KRaft quorum has no active controller, or more than one.
**Impact:** no metadata changes (topic changes, leader elections); clients keep working only while nothing changes.
**Diagnose:** controller logs in Loki: `{namespace="kafka"} |= "QuorumController"`; locally the dual-role nodes are both brokers and controllers, so a node failure affects both.
**Fix:** restore the quorum (a majority of controllers, 2 of 3).
