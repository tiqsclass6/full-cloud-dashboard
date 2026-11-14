#!/usr/bin/env bash
# Admission policy test via server-side dry runs: compliant pods pass, violations fail in platform namespaces only.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl jq
FAILED=0
# Checks run as `manifest | expect_...`: without lastpipe they run in a subshell and FAILED never reaches the exit code.
shopt -s lastpipe
IMAGE="${KAFKA_TOOLS_IMAGE}"     # an approved, pinned image

# pod <namespace> <image> <resources yes|no> [privileged yes|no]: a pod manifest on stdout
pod() {
  local res="" priv="allowPrivilegeEscalation: false
        capabilities: {drop: [\"ALL\"]}"
  [ "$3" = yes ] && res="resources: {requests: {cpu: 10m, memory: 32Mi}, limits: {memory: 64Mi}}"
  [ "${4:-no}" = yes ] && priv="privileged: true"
  cat <<EOF
apiVersion: v1
kind: Pod
metadata: {name: policy-test, namespace: $1}
spec:
  securityContext: {runAsNonRoot: true, runAsUser: 1001, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: c
      image: $2
      command: ["sleep", "60"]
      ${res}
      securityContext:
        ${priv}
EOF
}

admitted() { kc apply --dry-run=server -f - >/dev/null 2>"${ERR}"; }
ERR="$(mktemp)"; trap 'rm -f "${ERR}"' EXIT
expect_admitted() { if admitted; then ok "$1"; else fail "$1"; sed 's/^/        /' "${ERR}" | cut -c1-200; fi; }
expect_denied() {  # expect_denied <description> <regex the error must match>
  if admitted; then fail "$1 (admitted)"
  elif grep -qE "$2" "${ERR}"; then ok "$1"
  else fail "$1 (denied for another reason)"; sed 's/^/        /' "${ERR}" | cut -c1-200; fi
}

log "Platform namespace (apps): enforced"
pod apps "${IMAGE}" yes                          | expect_admitted "compliant pod is admitted"
pod apps "${IMAGE}" no                           | expect_denied "pod without requests/limits" "require-requests-limits"
pod apps "quay.io/strimzi/kafka:latest" yes      | expect_denied "image tagged :latest" "require-pinned-image-tags"
pod apps "quay.io/strimzi/kafka" yes             | expect_denied "image without a tag" "require-pinned-image-tags"
pod apps "docker.io/library/nginx:1.29.3" yes    | expect_denied "image from an unapproved source" "restrict-image-registries"
pod apps "${IMAGE}" yes yes                      | expect_denied "privileged container" "PodSecurity|privileged"
kc create deployment policy-test -n apps --image="${IMAGE}" --dry-run=client -o yaml \
                                                 | expect_denied "Deployment without requests/limits (autogen)" "require-requests-limits"
kc create service nodeport policy-test -n apps --tcp=80:80 --dry-run=client -o yaml \
                                                 | expect_denied "NodePort Service" "restrict-service-types"
lb() {  # lb <scheme> <allocate node ports>: a LoadBalancer Service
  printf 'apiVersion: v1\nkind: Service\nmetadata:\n  name: policy-test\n  namespace: apps\n  annotations: {service.beta.kubernetes.io/aws-load-balancer-scheme: %s}\nspec:\n  type: LoadBalancer\n  allocateLoadBalancerNodePorts: %s\n  selector: {app: none}\n  ports: [{port: 443}]\n' "$1" "$2"
}
lb internet-facing false                         | expect_denied "internet-facing load balancer" "restrict-service-types"
lb internal true                                 | expect_denied "internal load balancer with node ports" "restrict-service-types"
lb internal false                                | expect_admitted "internal load balancer without node ports is admitted"

log "ops-control: only copies of the platform's Spark jobs"
# spark_run <jq filter>: the compaction schedule's template as a SparkApplication, changed by the filter.
SCHEDULE="$(kc -n apps get scheduledsparkapplication events-compaction -o json)"
spark_run() {
  jq --arg ns apps "{apiVersion: \"sparkoperator.k8s.io/v1beta2\", kind: \"SparkApplication\",
    metadata: {name: \"policy-test\", namespace: \$ns}, spec: .spec.template} | $1" <<<"${SCHEDULE}"
}
# create, as the executor does (apply would first need get, which ops-control does not have).
as_control() { kc create --dry-run=server --as=system:serviceaccount:ai-ops:ops-control -f - >/dev/null 2>"${ERR}"; }
control_admitted() { if as_control; then ok "$1"; else fail "$1"; sed 's/^/        /' "${ERR}" | cut -c1-200; fi; }
control_denied() {
  if as_control; then fail "$1 (admitted)"
  elif grep -qE "$2" "${ERR}"; then ok "$1"
  else fail "$1 (denied for another reason)"; sed 's/^/        /' "${ERR}" | cut -c1-200; fi
}
spark_run '.'                                                  | control_admitted "a copy of the compaction schedule is admitted"
spark_run '.spec.driver.serviceAccount = "default"'            | control_denied "driver under another service account" "restrict-ops-control-spark"
spark_run '.spec.image = "docker.io/library/python:3.13"'      | control_denied "another image" "restrict-ops-control-spark"
spark_run '.spec.mainApplicationFile = "s3://elsewhere/x.py"'  | control_denied "a job file from outside the image" "restrict-ops-control-spark"
spark_run '.spec.sparkConf["spark.kubernetes.authenticate.driver.serviceAccountName"] = "flink"' \
                                                               | control_denied "a Spark identity override" "restrict-ops-control-spark"
spark_run '.metadata.namespace = "data"'                       | control_denied "a run outside apps (RBAC)" "forbidden"
kc -n apps get scheduledsparkapplication events-hourly-aggregate -o name >/dev/null 2>&1 \
  && { kc auth can-i get scheduledsparkapplication/events-hourly-aggregate -n apps --as=system:serviceaccount:ai-ops:ops-control >/dev/null 2>&1 \
         && fail "ops-control can read another schedule" || ok "ops-control reads only the compaction schedule"; }

log "Third-party namespace (monitoring): audited, not blocked"
pod monitoring "${IMAGE}" no                     | expect_admitted "pod without requests/limits is admitted"
pod monitoring "quay.io/strimzi/kafka:latest" yes | expect_admitted "image tagged :latest is admitted"

log "Argo CD projects: applications outside their grants are refused"
# argo_refused <name> <project> <path> <namespace> <regex>: an Application that must not sync.
argo_refused() {
  local name="$1" project="$2" path="$3" ns="$4" msg
  kc apply -f - >/dev/null <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: ${name}, namespace: argocd}
spec:
  project: ${project}
  source: {repoURL: "http://gitea-http.gitops.svc.cluster.local:3000/lakehouse/platform.git", targetRevision: main, path: ${path}}
  destination: {server: "https://kubernetes.default.svc", namespace: ${ns}}
  syncPolicy: {automated: {}}
EOF
  for _ in $(seq 1 20); do
    msg="$(kc -n argocd get application "${name}" -o jsonpath='{.status.conditions[*].message} {.status.operationState.message}' 2>/dev/null)"
    echo "${msg}" | grep -qE "$5" && break
    sleep 3
  done
  kc -n argocd delete application "${name}" --wait=false >/dev/null 2>&1
  if echo "${msg}" | grep -qE "$5"; then ok "$6"; else fail "$6"; echo "        ${msg:0:200}"; fi
}
argo_refused policy-test-default default gitops/bootstrap gitops \
  "not permitted|do not match any of the allowed|not allowed" "the default project can deploy nothing"
argo_refused policy-test-lakehouse lakehouse gitops/bootstrap gitops \
  "not permitted|do not match any of the allowed|not allowed" "lakehouse project cannot deploy to the gitops namespace"

echo
if [ "${FAILED}" -eq 0 ]; then log "All admission policy checks passed."; else log "Some admission policy checks FAILED"; exit 1; fi
