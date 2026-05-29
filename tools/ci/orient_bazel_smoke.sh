#!/usr/bin/env bash
set -euo pipefail

orient_bin="${1:?usage: orient_bazel_smoke.sh <orient-binary>}"
"${orient_bin}" tool-manifest | grep -q '"name":"search_code"'
