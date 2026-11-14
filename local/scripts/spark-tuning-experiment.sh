#!/usr/bin/env bash
# Shuffle-partition / AQE experiment on the hourly aggregate; prints the job's own timing per variant.
#   usage: spark-tuning-experiment.sh [runs per variant, default 2]
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl python

RUNS="${1:-2}"
printf '%-34s %4s %9s %8s %6s %6s\n' variant run seconds shuffle aqe files

run_variant() {  # run_variant <label> <spark conf lines>
  local label="$1" conf="$2"
  for i in $(seq 1 "${RUNS}"); do
    out="$(SPARK_CONF_EXTRA="${conf}" bash "${LOCAL_DIR}/scripts/spark-run.sh" "tune-$$-${i}" aggregate --lookback-hours 24 2>&1 | grep '^{' | tail -1)"
    if [ -z "${out}" ]; then printf '%-34s %4s %9s\n' "${label}" "${i}" "FAILED"; continue; fi
    echo "${out}" | python -c "
import sys, json
d = json.loads(sys.stdin.read())
print('%-34s %4s %9s %8s %6s %6s' % ('${label}', '${i}', d['seconds'], d['shuffle_partitions'], d['aqe'], d['target_data_files']))"
  done
}

run_variant "AQE off, 200 shuffle partitions" $'spark.sql.adaptive.enabled=false\nspark.sql.shuffle.partitions=200'
run_variant "AQE off, 8 shuffle partitions"   $'spark.sql.adaptive.enabled=false\nspark.sql.shuffle.partitions=8'
run_variant "AQE on,  200 (default)"          $'spark.sql.adaptive.enabled=true\nspark.sql.shuffle.partitions=200'
run_variant "AQE on,  8 shuffle partitions"   $'spark.sql.adaptive.enabled=true\nspark.sql.shuffle.partitions=8'
