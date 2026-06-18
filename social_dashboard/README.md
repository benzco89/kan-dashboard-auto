# Kan News — Social Dashboard

A FastAPI app that reads the shared Google Sheet (the one the collectors write
to) and serves a branded, RTL Hebrew dashboard for the news editor: an overview
plus per-platform pages for YouTube, Facebook and Instagram. Same shape as the
`push-mirror` service (uvicorn behind nginx + Authelia).

Live: **https://social.benzcohq.com** · links to the push mirror at
https://pushstat.benzcohq.com from the header.

## Layout

```
server.py        FastAPI: serves the HTML pages + JSON API
gsheets.py       Google Sheets read layer (service account, TTL cache)
aggregate.py     turns sheet rows into per-page metrics (source of truth)
templates/       overview / youtube / facebook / instagram HTML (vanilla JS)
static/          app.css, app.js, fonts (SimplerPro)
deploy/          systemd unit + nginx site
```

The browser only formats and draws; all numbers are computed in `aggregate.py`.

## Data model

Data is collected ~08:30 Israel time, so the latest data day is *yesterday*. A
range of N days means `[today-N, yesterday]`; the previous-period comparison is
the N days before that. Sheet data is cached in memory for `CACHE_TTL_SECONDS`
(default 600); the header refresh button forces a reload (`?refresh=1`).

## Run locally

```bash
python -m venv venv
venv/Scripts/python -m pip install -r requirements.txt   # (Linux: venv/bin/...)
# credentials: env GCP_SERVICE_ACCOUNT (JSON), or SERVICE_ACCOUNT_FILE, or
# a service-account.json next to the app / in the parent dir.
venv/Scripts/python -m uvicorn server:app --port 8430
```

Open http://127.0.0.1:8430/.

## Endpoints

- `/`, `/youtube`, `/facebook`, `/instagram` — pages
- `/api/{overview,youtube,facebook,instagram}?days=7|14|30|90[&refresh=1]` — JSON
- `/health`

## Env

- `GCP_SERVICE_ACCOUNT` — service-account JSON (string) **or**
- `SERVICE_ACCOUNT_FILE` — path to the JSON key file
- `CACHE_TTL_SECONDS` — sheet cache TTL (default 600)
- `PUSHSTAT_URL` — push-mirror link in the header (default https://pushstat.benzcohq.com/)

## Deploy (Hetzner VPS)

Code in `/opt/social-dashboard`, run by `social-dashboard.service` (uvicorn on
127.0.0.1:8430), proxied by nginx (`deploy/social.benzcohq.com.nginx`) with a
Let's Encrypt cert from `certbot --nginx -d social.benzcohq.com`, behind the
same Authelia SSO as pushstat. Fonts are shared from `/opt/push-mirror/brand/fonts`.
