#!/usr/bin/env bash
# Delete the local kind cluster (and all data in it).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kind
kind delete cluster --name "${CLUSTER_NAME}"
