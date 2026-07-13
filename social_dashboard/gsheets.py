"""
Google Sheets data layer for the Kan News social dashboard.

Reads the same spreadsheet the collectors write to (read-only) and exposes
parsed, header-keyed rows with a short in-memory TTL cache. The data only
changes once a day (collectors run ~08:30 Israel time), so a few minutes of
caching keeps us well inside Sheets API quotas.
"""

import json
import os
import threading
import time

from google.oauth2 import service_account
from googleapiclient.discovery import build

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"

# Hebrew tab names in the spreadsheet
SHEETS = {
    "youtube": "נתוני יוטיוב",
    "facebook": "נתוני פייסבוק",
    "instagram": "נתוני אינסטגרם",
    "stories": "סטוריז אינסטגרם",
    "twitter": "נתוני טוויטר",
    "followers": "מעקב עוקבים",
    "insights": "תובנות יומיות",
    "top_combined": "Top Combined",
}

# Fetched separately from the batchGet: these tabs are created lazily
# (by comment_analyzer.py / hot_sniffer.py), and a missing range would
# 400 the whole batch.
LAZY_SHEETS = {
    "comment_analysis": "ניתוח תגובות",
    "hot_alerts": "hot_alerts",
    "competitors": "מתחרים",
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

_CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", "600"))

_lock = threading.Lock()
_cache = {"data": None, "ts": 0.0}


def _credentials():
    """Service-account creds from env var (prod) or local file (dev)."""
    raw = os.environ.get("GCP_SERVICE_ACCOUNT")
    if raw:
        info = json.loads(raw)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    for path in (
        os.environ.get("SERVICE_ACCOUNT_FILE"),
        os.path.join(os.path.dirname(__file__), "service-account.json"),
        os.path.join(os.path.dirname(__file__), "..", "service-account.json"),
    ):
        if path and os.path.exists(path):
            return service_account.Credentials.from_service_account_file(path, scopes=SCOPES)

    raise RuntimeError(
        "No Google credentials found. Set GCP_SERVICE_ACCOUNT or provide service-account.json"
    )


def _rows_to_dicts(values):
    """Turn a raw value matrix (first row = header) into a list of dicts."""
    if not values or len(values) < 2:
        return []
    header = [(h or "").strip() for h in values[0]]
    out = []
    for row in values[1:]:
        item = {}
        for i, key in enumerate(header):
            if not key:
                continue
            item[key] = row[i] if i < len(row) else ""
        out.append(item)
    return out


def _fetch_all():
    """One batched read of every tab we use."""
    creds = _credentials()
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    ranges = list(SHEETS.values())
    resp = (
        service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=SPREADSHEET_ID, ranges=ranges)
        .execute()
    )
    value_ranges = resp.get("valueRanges", [])

    # The API echoes ranges back in request order, so map by index.
    result = {}
    for idx, key in enumerate(SHEETS.keys()):
        result[key] = value_ranges[idx].get("values", []) if idx < len(value_ranges) else []

    # Top Combined has no header row: [title, type, views, platform]
    top = []
    for r in result.get("top_combined", []):
        if len(r) >= 4:
            top.append({
                "title": r[0],
                "type": r[1],
                "views": r[2],
                "platform": r[3],
            })

    lazy = {}
    for key, sheet_name in LAZY_SHEETS.items():
        lazy[key] = []
        try:
            resp = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=SPREADSHEET_ID, range=sheet_name)
                .execute()
            )
            lazy[key] = _rows_to_dicts(resp.get("values", []))
        except Exception:
            pass

    return {
        "youtube": _rows_to_dicts(result.get("youtube", [])),
        "facebook": _rows_to_dicts(result.get("facebook", [])),
        "instagram": _rows_to_dicts(result.get("instagram", [])),
        "stories": _rows_to_dicts(result.get("stories", [])),
        "twitter": _rows_to_dicts(result.get("twitter", [])),
        "followers": _rows_to_dicts(result.get("followers", [])),
        "insights": _rows_to_dicts(result.get("insights", [])),
        "top_combined": top,
        **lazy,
    }


def get_data(force=False):
    """Return all parsed sheet data, using the TTL cache unless forced."""
    now = time.time()
    with _lock:
        fresh = _cache["data"] is not None and (now - _cache["ts"]) < _CACHE_TTL
        if fresh and not force:
            return _cache["data"]
    # fetch outside the lock so concurrent requests don't serialize on the network
    data = _fetch_all()
    with _lock:
        _cache["data"] = data
        _cache["ts"] = time.time()
    return data
