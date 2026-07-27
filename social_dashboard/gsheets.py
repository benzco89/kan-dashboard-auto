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
    "tiktok": "נתוני טיקטוק",
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
    "competitor_posts": "פוסטים מתחרים",
    "demographics": "דמוגרפיה",
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

ALL_SHEETS = {**SHEETS, **LAZY_SHEETS}

_CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", "600"))

_lock = threading.Lock()
# per-TAB cache: {key: (rows, fetched_at)}. One blob meant any page
# refetched all 14 tabs the moment the oldest one expired.
_cache = {}


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


def _parse(key, values):
    """Raw value matrix -> rows. Top Combined has no header row."""
    if key != "top_combined":
        return _rows_to_dicts(values)
    return [{"title": r[0], "type": r[1], "views": r[2], "platform": r[3]}
            for r in values if len(r) >= 4]


def _service():
    return build("sheets", "v4", credentials=_credentials(), cache_discovery=False)


def _fetch(keys):
    """Fetch exactly these sheet keys, in ONE call when possible.

    The lazy tabs used to be fetched one at a time, outside the batch, because
    they are created on demand by other scripts and a missing range 400s the
    whole batch. That cost ~2 of the original 6.3 seconds. They are all present
    now, so they join the batch and the whole read is a single round trip — with
    a per-sheet fallback for the day one of them does not exist yet.

    Deliberately NOT threaded: a googleapiclient service is not thread-safe, and
    sharing one across a pool segfaulted the process outright.
    """
    svc = _service()
    ranges = [ALL_SHEETS[k] for k in keys if k in ALL_SHEETS]
    if not ranges:
        return {}
    ordered = [k for k in keys if k in ALL_SHEETS]
    try:
        resp = (svc.spreadsheets().values()
                .batchGet(spreadsheetId=SPREADSHEET_ID, ranges=ranges).execute())
        vrs = resp.get("valueRanges", [])
        return {k: _parse(k, vrs[i].get("values", []) if i < len(vrs) else [])
                for i, k in enumerate(ordered)}
    except Exception:
        pass

    # one tab does not exist yet -> ask for them one by one so the rest survive
    out = {}
    for k in ordered:
        try:
            r = (svc.spreadsheets().values()
                 .get(spreadsheetId=SPREADSHEET_ID, range=ALL_SHEETS[k]).execute())
            out[k] = _parse(k, r.get("values", []))
        except Exception:
            out[k] = []
    return out


def _fresh(key, now):
    hit = _cache.get(key)
    return hit is not None and (now - hit[1]) < _CACHE_TTL


def _load(keys, force=False):
    """Make sure every key is in the cache, fetching only what is missing."""
    now = time.time()
    with _lock:
        need = [k for k in keys if force or not _fresh(k, now)]
    if need:
        fetched = _fetch(need)
        stamp = time.time()
        with _lock:
            for k, rows in fetched.items():
                _cache[k] = (rows, stamp)
    _start_warmer()
    with _lock:
        return {k: _cache[k][0] for k in keys if k in _cache}


# ---------------------------------------------------------------- warmer

# Refresh before the entry expires rather than when someone asks for it. The
# cache made a warm page 190-550ms and a cold one 0.3-3.6s (measured live
# 2026-07-27, /api/overview being both the slowest and the landing page) — and
# with a 600s TTL over data that changes once a day, the first visitor after ten
# quiet minutes paid that cost on everyone's behalf.
#
# The margin is what makes it work: refreshing at TTL-90s means an entry is
# replaced while it is still valid, so a request never finds a missing one.
_WARM_MARGIN = 90
_warmer = None


def _warm_once():
    """Re-read every tab already in the cache. Only tabs someone has asked for —
    warming all fourteen would undo the per-page split that made this fast."""
    with _lock:
        keys = list(_cache.keys())
    if not keys:
        return
    fetched = _fetch(keys)
    stamp = time.time()
    with _lock:
        for k, rows in fetched.items():
            # a failed read returns [] for that tab; keeping the previous rows is
            # better than serving an empty dashboard because Sheets hiccuped
            if rows or not _cache.get(k, ([], 0))[0]:
                _cache[k] = (rows, stamp)


def _warm_loop(interval):
    while True:
        time.sleep(interval)
        try:
            _warm_once()
        except Exception as e:      # never let a bad read kill the thread
            print(f"[gsheets] warm failed: {str(e)[:120]}", flush=True)


def _start_warmer():
    """Started by the first real read, not at import: a CLI or a test that
    imports this module should not spawn a thread or touch the network."""
    global _warmer
    if _warmer is not None or os.environ.get("CACHE_WARM", "1") == "0":
        return
    interval = max(30, _CACHE_TTL - _WARM_MARGIN)
    with _lock:
        if _warmer is not None:
            return
        _warmer = threading.Thread(target=_warm_loop, args=(interval,),
                                   name="gsheets-warmer", daemon=True)
        _warmer.start()
    print(f"[gsheets] cache warmer every {interval}s (TTL {_CACHE_TTL}s)", flush=True)


class SheetData(dict):
    """Sheet rows by key, which fetches a sheet nobody asked for on demand.

    Each page declares the tabs it needs so they arrive in one round trip — a
    platform page needs 2-5 of the 14 and used to pay for all of them. But a
    declaration that turns out to be wrong must not render an empty section: an
    unexpected key costs one extra call here instead of silently being blank,
    which is the failure mode this whole shape could otherwise introduce.
    """

    def get(self, key, default=None):
        if key not in self and key in ALL_SHEETS:
            self.update(_load([key]))
        return super().get(key, default)

    def __getitem__(self, key):
        if key not in self and key in ALL_SHEETS:
            self.update(_load([key]))
        return super().__getitem__(key)


def get_data(force=False, keys=None):
    """Sheet data for the tabs in `keys` (default: all of them).

    Returns a SheetData, so a caller that reaches for a tab outside `keys` still
    gets it. Cache is per-tab: opening Facebook and then Instagram pays only for
    what the second page adds.
    """
    wanted = list(keys) if keys else list(ALL_SHEETS)
    return SheetData(_load(wanted, force=force))
