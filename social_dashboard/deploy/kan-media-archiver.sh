#!/usr/bin/env bash
# Trigger the Media Archiver workflow at reliable local times. GitHub's own
# schedule: queue fires hours late, which is why every intraday job in this
# repo is dispatched from here instead (same pattern as kan-hot-sniffer and
# kan-daily-dispatch). Token from /etc/kan-dispatch.env.
#
# ARCHIVE_MODE is set by the unit that calls this: kan-media-archiver leaves it
# unset, kan-media-reconcile sets "reconcile". It is a bare word and not JSON
# on purpose - systemd's Environment= does its own quote parsing, so a value
# like {"reconcile":"1"} can arrive with its quotes eaten and produce a body
# the API rejects with a 422 nobody reads. The script builds the JSON itself,
# from a closed set of two, and refuses anything else.
set -euo pipefail
: "${GH_DISPATCH_TOKEN:?missing GH_DISPATCH_TOKEN}"

REPO="benzco89/kan-dashboard-auto"
WORKFLOW="media_archiver.yml"
case "${ARCHIVE_MODE:-archive}" in
  reconcile) INPUTS='{"reconcile":"1"}' ;;
  prune)     INPUTS='{"audit":"1","prune":"1"}' ;;
  archive)   INPUTS='{}' ;;
  *) echo "unknown ARCHIVE_MODE: ${ARCHIVE_MODE}" >&2; exit 2 ;;
esac

code=$(curl -s -o /tmp/kan-media-archiver.out -w '%{http_code}' -X POST \
  -H "Authorization: Bearer ${GH_DISPATCH_TOKEN}" \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches" \
  -d "{\"ref\":\"main\",\"inputs\":${INPUTS}}")

if [ "$code" = "204" ]; then
  echo "$(date -Is) dispatched ${WORKFLOW} on main with ${INPUTS} (HTTP 204)"
else
  echo "$(date -Is) dispatch FAILED (HTTP ${code}): $(cat /tmp/kan-media-archiver.out)"
  exit 1
fi
