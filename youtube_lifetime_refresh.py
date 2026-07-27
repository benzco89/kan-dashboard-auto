#!/usr/bin/env python3
"""Bring the frozen half of the YouTube sheet back up to date.

The collector refreshes a video while it is younger than 30 days and then never
looks at it again. That window is the right length — a video adds only ~0.3%
(median) in the fortnight after it leaves — but the tail does not stop, it just
runs slowly for months. Measured on 2026-07-27 by `youtube_tail_probe.py` across
2,778 frozen videos: **118.8M stored against 127.8M actual, 8.9M views the
system cannot see.** 11% of them had gained over 10% since the cut; one was up
330% because it got rediscovered.

So this fills a SEPARATE column. It never touches `views`, and that is the whole
design, not a detail:

  `views` is "what this video did in its first 30 days" — every video measured
  on the same clock, which is exactly what makes one week comparable to another.
  Letting it keep growing would bias every week-over-week comparison downward,
  permanently and increasingly, because last week's number would keep rising
  while this week's is still fresh. It would also make `views_delta` fire on
  old rows and confuse the alerts and the hot sniffer, which were calibrated on
  the current behaviour. Same reason `engagement_rate` was left alone in the
  collectors.

`views_lifetime` answers a different question — "how much has this video earned
in total, to date" — and lives beside it instead of on top of it.

Cost: one call per 50 videos, 1 quota unit each. ~64 units of a 10,000/day quota.

Writes ONLY its own two columns, by range, so it cannot shift a row or a header.

    python youtube_lifetime_refresh.py            # refresh
    python youtube_lifetime_refresh.py --dry-run  # fetch and report, write nothing

Env: YOUTUBE_API_KEY, GCP_SERVICE_ACCOUNT (or service-account.json).
"""

import os
import sys
import json
from datetime import datetime

import requests
import pytz

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET = "נתוני יוטיוב"
API = "https://www.googleapis.com/youtube/v3/videos"
COLUMNS = ["views_lifetime", "lifetime_checked"]
IL_TZ = pytz.timezone("Asia/Jerusalem")


def col_letter(n):
    """1 -> A, 27 -> AA."""
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def open_sheet():
    import gspread
    from google.oauth2.service_account import Credentials
    raw = os.environ.get("GCP_SERVICE_ACCOUNT") or os.environ.get("GOOGLE_CREDENTIALS")
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    if raw:
        creds = Credentials.from_service_account_info(json.loads(raw), scopes=scopes)
    else:
        path = next((p for p in ("service-account.json",
                                 os.path.join("social_dashboard", "service-account.json"))
                     if os.path.exists(p)), None)
        if not path:
            raise SystemExit("❌ no credentials: set GCP_SERVICE_ACCOUNT")
        creds = Credentials.from_service_account_file(path, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET)


def fetch_live(video_ids, api_key):
    live, missing = {}, 0
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        r = requests.get(API, params=dict(part="statistics", id=",".join(chunk),
                                          key=api_key), timeout=30)
        if r.status_code != 200:
            print("   ⚠️ API %s on batch %d: %s" % (r.status_code, i // 50 + 1, r.text[:140]))
            continue
        got = set()
        for item in r.json().get("items", []):
            vc = item.get("statistics", {}).get("viewCount")
            if vc is not None:
                live[item["id"]] = int(vc)
                got.add(item["id"])
        missing += len(set(chunk) - got)
    return live, missing


def main():
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        raise SystemExit("❌ missing YOUTUBE_API_KEY")
    dry = "--dry-run" in sys.argv

    print("=" * 62)
    print("♻️  YouTube lifetime refresh — %s" % datetime.now(IL_TZ).strftime("%Y-%m-%d %H:%M"))
    print("=" * 62)

    ws = open_sheet()
    values = ws.get_all_values()
    if not values:
        raise SystemExit("❌ the sheet came back empty")
    header, rows = values[0], values[1:]

    try:
        id_i = header.index("video_id")
        views_i = header.index("views")
    except ValueError:
        raise SystemExit("❌ no video_id/views column — header changed?")

    # the two columns are appended once and then reused
    added = [c for c in COLUMNS if c not in header]
    if added and not dry:
        for c in added:
            header.append(c)
        ws.update(values=[header], range_name="A1", value_input_option="RAW")
        print("   ➕ עמודות חדשות בסוף הכותרת: %s" % ", ".join(added))
    idx = {c: (header.index(c) if c in header else None) for c in COLUMNS}

    ids = [r[id_i].strip() for r in rows if len(r) > id_i and r[id_i].strip()]
    print("\n📄 %d סרטונים בגיליון · %d קריאות API" % (len(ids), (len(ids) + 49) // 50))
    live, missing = fetch_live(ids, api_key)
    print("   %d ענו, %d לא (נמחקו/פרטיים/מוגבלים)" % (len(live), missing))

    today = datetime.now(IL_TZ).strftime("%Y-%m-%d")
    out, stored_sum, live_sum, grew = [], 0, 0, 0
    for r in rows:
        vid = r[id_i].strip() if len(r) > id_i else ""
        cur = live.get(vid)
        if cur is None:
            # keep whatever was there; a video the API will not answer for must
            # not have its previous figure replaced by a blank
            prev = r[idx["views_lifetime"]] if idx["views_lifetime"] is not None and len(r) > idx["views_lifetime"] else ""
            prevd = r[idx["lifetime_checked"]] if idx["lifetime_checked"] is not None and len(r) > idx["lifetime_checked"] else ""
            out.append([prev, prevd])
            continue
        try:
            stored = int(float(str(r[views_i]).replace(",", "") or 0))
        except (ValueError, IndexError):
            stored = 0
        stored_sum += stored
        live_sum += cur
        if stored and cur > stored * 1.1:
            grew += 1
        out.append([cur, today])

    gap = live_sum - stored_sum
    print("\n   בגיליון %s · במציאות %s" % (format(stored_sum, ","), format(live_sum, ",")))
    print("   הפרש %s צפיות (%.1f%%) · %d סרטונים צמחו מעל 10%%"
          % (format(gap, ","), (gap / stored_sum * 100) if stored_sum else 0, grew))

    if dry:
        print("\n(dry-run — לא נכתב כלום)")
        return

    first, last = idx["views_lifetime"] + 1, idx["lifetime_checked"] + 1
    if last != first + 1:
        raise SystemExit("❌ the two columns are not adjacent — refusing to write by range")
    rng = "%s2:%s%d" % (col_letter(first), col_letter(last), len(rows) + 1)
    ws.update(values=out, range_name=rng, value_input_option="RAW")
    print("\n   ✅ נכתבו %d שורות אל %s — ורק אליו" % (len(out), rng))


if __name__ == "__main__":
    main()
