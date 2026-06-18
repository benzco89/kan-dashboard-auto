#!/usr/bin/env bash
# Auto-deploy the social dashboard from git. Idempotent: only pulls + restarts
# when origin/main moved. Run periodically by social-deploy.timer.
# The app runs from $APP; the git checkout lives in $REPO. venv / creds / config
# stay in $APP and are never overwritten.
set -euo pipefail

# Serialize: never let two deploys (timer + manual, or overlapping ticks) collide.
exec 9>/var/lock/kan-social-deploy.lock
flock -n 9 || { echo "$(date -Is) deploy already running, skipping"; exit 0; }

REPO=/opt/kan-dashboard-auto
APP=/opt/social-dashboard
BRANCH=main

cd "$REPO"
git fetch --quiet origin "$BRANCH"
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse "origin/$BRANCH")
if [ "$LOCAL" = "$REMOTE" ]; then
    exit 0   # nothing new
fi

echo "$(date -Is) deploying $LOCAL -> $REMOTE"
git reset --hard "origin/$BRANCH" --quiet

rsync -a --delete \
    --exclude venv --exclude __pycache__ --exclude '*.pyc' \
    --exclude service-account.json --exclude config.json \
    --exclude static/fonts \
    "$REPO/social_dashboard/" "$APP/"

"$APP/venv/bin/pip" install -q -r "$APP/requirements.txt"
systemctl restart social-dashboard.service
echo "$(date -Is) deployed $REMOTE, service restarted"
