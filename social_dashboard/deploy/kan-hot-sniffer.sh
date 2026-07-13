#!/usr/bin/env bash
# Trigger the Hot Sniffer workflow every few hours at reliable local times
# (GitHub's scheduled-event queue fires hours late; same pattern as
# kan-daily-dispatch). Token from /etc/kan-dispatch.env.
set -euo pipefail
: "${GH_DISPATCH_TOKEN:?missing GH_DISPATCH_TOKEN}"

REPO="benzco89/kan-dashboard-auto"
WORKFLOW="hot_sniffer.yml"

code=$(curl -s -o /tmp/kan-hot-sniffer.out -w '%{http_code}' -X POST \
  -H "Authorization: Bearer ${GH_DISPATCH_TOKEN}" \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches" \
  -d '{"ref":"main"}')

if [ "$code" = "204" ]; then
  echo "$(date -Is) dispatched ${WORKFLOW} on main (HTTP 204)"
else
  echo "$(date -Is) dispatch FAILED (HTTP ${code}): $(cat /tmp/kan-hot-sniffer.out)"
  exit 1
fi
