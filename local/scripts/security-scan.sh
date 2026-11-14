#!/usr/bin/env bash
# Trivy image and IaC scans with SBOMs; gates on fixable HIGH/CRITICAL in our layers (accepted risks: .trivyignore.yaml).
#   usage: security-scan.sh [tag]     default: dev
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require trivy jq helm
FAILED=0
TAG="${1:-dev}"
OUT="${REPO_ROOT}/build/security"
IGNORE="${REPO_ROOT}/.trivyignore.yaml"
mkdir -p "${OUT}"

# Layers we control, as path prefixes inside each image.
declare -A OWNED=(
  [ingestion-service]="opt/ingestion-service/"
  [flink-iceberg-job]="opt/flink/usrlib/"
  [spark-jobs]="opt/jobs/ opt/spark/jars/iceberg-"
  [iceberg-exporter]="opt/iceberg-exporter/"
  # Python images: every installed package is one we chose (hash-locked requirements.txt).
  [mcp-platform-tools]="usr/local/lib/python3 opt/"
  [ai-ops]="usr/local/lib/python3 opt/"
  [hermes]="usr/local/lib/python3 opt/"
)

for name in ingestion-service flink-iceberg-job spark-jobs iceberg-exporter mcp-platform-tools ai-ops hermes; do
  image="lakehouse/${name}:${TAG}"
  log "${image}"
  trivy image --quiet --scanners vuln --ignorefile "${IGNORE}" --format json \
    -o "${OUT}/${name}.vulns.json" "${image}" || { fail "scan failed"; continue; }
  trivy image --quiet --format cyclonedx -o "${OUT}/${name}.cdx.json" "${image}" \
    && ok "SBOM: build/security/${name}.cdx.json ($(jq '.components | length' "${OUT}/${name}.cdx.json") components)"
  jq -r '[.Results[]?.Vulnerabilities[]?] |
         "report: \(length) findings (critical \([.[]|select(.Severity=="CRITICAL")]|length), high \([.[]|select(.Severity=="HIGH")]|length), fixable \([.[]|select((.FixedVersion // "") != "")]|length))"' \
    "${OUT}/${name}.vulns.json" | sed 's/^/        /'
  gate="$(jq -r --arg owned "${OWNED[${name}]}" '
      ($owned | split(" ")) as $prefixes
      | .Results[]? | .Class as $class | .Vulnerabilities[]?
      | select(.Severity == "HIGH" or .Severity == "CRITICAL")
      | select((.FixedVersion // "") != "")
      | select($class == "os-pkgs" or ((.PkgPath // "") as $p | [$prefixes[] | . as $x | $p | startswith($x)] | any))
      | "\(.Severity) \(.VulnerabilityID) \(.PkgName) \(.InstalledVersion) -> \(.FixedVersion) (\(.PkgPath // "os"))"' \
      "${OUT}/${name}.vulns.json" | sort -u)"
  if [ -z "${gate}" ]; then ok "gate: no fixable HIGH/CRITICAL findings in our layers"
  else fail "gate: $(echo "${gate}" | wc -l) fixable HIGH/CRITICAL finding(s) in our layers"; echo "${gate}" | sed 's/^/        /'; fi
done

log "Terraform misconfigurations"
if trivy config --quiet --severity HIGH,CRITICAL --ignorefile "${IGNORE}" --exit-code 1 \
     --skip-dirs "**/.terraform" \
     --format json -o "${OUT}/terraform.misconfig.json" "${REPO_ROOT}/terraform"; then
  ok "no HIGH/CRITICAL misconfigurations"
else
  fail "HIGH/CRITICAL misconfigurations"
  jq -r '.Results[]? | .Target as $t | .Misconfigurations[]? | "        \(.Severity) \(.ID) \(.Title) (\($t))"' \
    "${OUT}/terraform.misconfig.json" | sort -u
fi

log "Helm charts (AWS dev overlay)"
if trivy config --quiet --severity HIGH,CRITICAL --ignorefile "${IGNORE}" --exit-code 1 \
     --helm-values "${REPO_ROOT}/helm/values/aws.yaml" --helm-values "${REPO_ROOT}/helm/values/dev.yaml" \
     --format json -o "${OUT}/helm.misconfig.json" "${REPO_ROOT}/helm/charts/platform"; then
  ok "no HIGH/CRITICAL misconfigurations ($(jq '[.Results[]?] | length' "${OUT}/helm.misconfig.json") templates)"
else
  fail "HIGH/CRITICAL misconfigurations"
  jq -r '.Results[]? | .Target as $t | .Misconfigurations[]? | "        \(.Severity) \(.ID) \(.Title) (\($t))"' \
    "${OUT}/helm.misconfig.json" | sort -u
fi

echo
if [ "${FAILED}" -eq 0 ]; then log "Security scan passed. Reports and SBOMs: build/security/"; else log "Security scan FAILED"; exit 1; fi
