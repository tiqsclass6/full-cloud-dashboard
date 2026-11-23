#!/usr/bin/env bash
# Memory and CPU per pod and namespace, from Prometheus (the cluster has no metrics-server, so kubectl top
# does not work). The Docker VM line shows what the nodes use in total.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl curl jq
set +e
fwd monitoring svc/kps-prometheus 9090:9090
sleep 5
q() { curl -s --max-time 30 -G --data-urlencode "query=$1" http://localhost:9090/api/v1/query; }
table() {  # table <title> <promql> <unit divisor> <unit>
  echo "== $1"
  q "$2" | jq -r --argjson d "$3" --arg u "$4" '.data.result | sort_by(-(.value[1]|tonumber)) | .[]
    | "\(.metric.namespace // "-")\t\(.metric.pod // .metric.node // "")\t\((.value[1]|tonumber)/$d | floor)\($u)"' | column -t -s $'\t' | head -"${5:-30}"
}
W='container!="",container!="POD",image!=""'
table "memory working set by pod" "topk(30, sum by (namespace, pod) (container_memory_working_set_bytes{$W}))" 1048576 Mi
table "cpu by pod (5m avg, millicores)" "topk(20, sum by (namespace, pod) (rate(container_cpu_usage_seconds_total{$W}[5m])))" 0.001 m 20
table "memory by namespace" "sort_desc(sum by (namespace) (container_memory_working_set_bytes{$W}))" 1048576 Mi
table "cpu by namespace (millicores)" "sort_desc(sum by (namespace) (rate(container_cpu_usage_seconds_total{$W}[5m])))" 0.001 m
table "memory requests by namespace" "sort_desc(sum by (namespace) (kube_pod_container_resource_requests{resource=\"memory\"}))" 1048576 Mi
table "memory by node" "sum by (node) (container_memory_working_set_bytes{$W})" 1048576 Mi
echo "== pods running: $(kc get pods -A --no-headers --field-selector=status.phase=Running | wc -l)"
echo "== Docker VM: $(docker exec lakehouse-control-plane grep MemAvailable /proc/meminfo | tr -s " ")"; docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}'
stop_forwards
