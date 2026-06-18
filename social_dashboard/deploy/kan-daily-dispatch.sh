#!/usr/bin/env bash
# Reliably trigger the "Daily Social Update" workflow at the scheduled local time,
# bypassing GitHub's unreliable scheduled-event queue (which fires hours late).
# Token comes from /etc/kan-dispatch.env (GH_DISPATCH_TOKEN: fine-grained PAT,
# repo benzco89/kan-dashboard-auto, Actions: read+write).
set -euo pipefail
: "${GH_DISPATCH_TOKEN:?missing GH_DISPATCH_TOKEN}"

REPO="benzco89/kan-dashboard-auto"
WORKFLOW="daily_update.yml"

code=$(curl -s -o /tmp/kan-dispatch.out -w '%{http_code}' -X POST \
  -H "Authorization: Bearer ${GH_DISPATCH_TOKEN}" \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches" \
  -d '{"ref":"main"}')

if [ "$code" = "204" ]; then
  echo "$(date -Is) dispatched ${WORKFLOW} on main (HTTP 204)"
else
  echo "$(date -Is) dispatch FAILED (HTTP ${code}): $(cat /tmp/kan-dispatch.out)"
  exit 1
fi
