"""
Kan News — Social analytics dashboard (FastAPI).

Serves four static HTML pages (overview + per-platform) and a small JSON API
that reads the shared Google Sheet and returns aggregated metrics. Mirrors the
deployment shape of the push-mirror service (uvicorn behind nginx/Authelia).
"""

import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import aggregate
import gsheets

BASE_DIR = os.path.dirname(__file__)
TEMPLATES = os.path.join(BASE_DIR, "templates")
STATIC = os.path.join(BASE_DIR, "static")

# Cross-link target for the push-mirror site (shown in the header)
PUSHSTAT_URL = os.environ.get("PUSHSTAT_URL", "https://pushstat.benzcohq.com/")

app = FastAPI(title="Kan Social Dashboard", docs_url=None, redoc_url=None)


@app.middleware("http")
async def revalidate_assets(request, call_next):
    """Force browsers to revalidate static assets + HTML pages on every load.

    The dashboard has no build step / asset hashing, so a cached app.js/app.css
    from a previous deploy would otherwise stick (missing icons, stale nav).
    'no-cache' keeps the file cached but always revalidates via the ETag
    (cheap 304 when unchanged, fresh 200 right after a deploy).
    """
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/static/") or path in ("/", "/youtube", "/facebook", "/instagram", "/twitter"):
        response.headers["Cache-Control"] = "no-cache"
    return response


if os.path.isdir(STATIC):
    app.mount("/static", StaticFiles(directory=STATIC), name="static")

_ALLOWED_RANGES = {7, 14, 30, 90}
_BUILDERS = {
    "overview": aggregate.build_overview,
    "youtube": aggregate.build_youtube,
    "facebook": aggregate.build_facebook,
    "instagram": aggregate.build_instagram,
    "twitter": aggregate.build_twitter,
}


def _page(name):
    path = os.path.join(TEMPLATES, name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="page not found")
    return FileResponse(path)


@app.get("/")
def overview_page():
    return _page("overview.html")


@app.get("/youtube")
def youtube_page():
    return _page("youtube.html")


@app.get("/facebook")
def facebook_page():
    return _page("facebook.html")


@app.get("/instagram")
def instagram_page():
    return _page("instagram.html")


@app.get("/twitter")
def twitter_page():
    return _page("twitter.html")


@app.get("/api/config")
def api_config():
    return {"pushstat_url": PUSHSTAT_URL}


@app.get("/api/{page}")
def api_page(page: str, days: int = Query(7), refresh: int = Query(0)):
    builder = _BUILDERS.get(page)
    if builder is None:
        raise HTTPException(status_code=404, detail="unknown page")
    if days not in _ALLOWED_RANGES:
        days = 7
    try:
        data = gsheets.get_data(force=bool(refresh))
        payload = builder(data, days)
        payload["pushstat_url"] = PUSHSTAT_URL
        return JSONResponse(payload)
    except Exception as exc:  # surface a clean error to the client
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/health")
def health():
    try:
        data = gsheets.get_data()
        ok = bool(data.get("followers"))
        return {"ok": ok, "last_date": aggregate._last_data_date(data)}
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
