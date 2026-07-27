#!/usr/bin/env python3
"""Does the YouTube collector's 30-day window cut videos while they are still growing?

READ-ONLY. Never writes to Google Sheets and never touches a collector.

The question came out of a measurement of the other platforms: a Facebook,
Instagram, TikTok or X post is finished by about its fourth day — the whole week
of 12-18/07 gained 0.2-0.7% between day 4 and its freeze. YouTube is the one
platform with a real tail: the same week gained 6.0% over that span and was still
adding ~1%/day at age 9-15 days. YouTube is also the one collector with a 30-day
window rather than 7. So: is 30 days enough, or does it freeze videos mid-climb?

The sheet cannot answer this on its own. A video stops being refreshed once it is
older than 30 days, so its stored `views` is frozen at whatever it had then — no
amount of waiting makes that number move again. The live figure has to come from
the YouTube API.

Which makes the measurement immediate rather than a week away: the sheet already
holds videos that froze months ago. Ask the API what they have TODAY, and the gap
is exactly what the 30-day cut is hiding.

    python youtube_tail_probe.py            # measure
    python youtube_tail_probe.py --csv out.csv   # also dump per-video rows

Env: YOUTUBE_API_KEY, and GCP_SERVICE_ACCOUNT (or a local service-account.json).
"""

import os
import sys
import json
import csv
import statistics
from datetime import datetime, timezone

import requests

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET = "נתוני יוטיוב"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
API = "https://www.googleapis.com/youtube/v3/videos"

# The collector refreshes a video while it is younger than this.
WINDOW_DAYS = 30

# A video needs a couple of days past the window before its sheet value is
# certainly frozen (the last refresh happens on the last run inside the window).
FROZEN_AFTER = 33

# Videos still inside the window. Their sheet value should match the API almost
# exactly — that is the control: if these disagree, the method is measuring
# collector lag or a broken join, not a tail.
CONTROL_MAX_AGE = 25

BUCKETS = [(33, 45), (45, 60), (60, 90), (90, 180), (180, 100000)]


def fetch_sheet():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    raw = os.environ.get("GCP_SERVICE_ACCOUNT")
    if raw:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(raw), scopes=SCOPES)
    else:
        path = next((p for p in ("service-account.json",
                                 os.path.join("social_dashboard", "service-account.json"))
                     if os.path.exists(p)), None)
        if not path:
            raise SystemExit("no credentials: set GCP_SERVICE_ACCOUNT or add service-account.json")
        creds = service_account.Credentials.from_service_account_file(path, scopes=SCOPES)

    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    values = (svc.spreadsheets().values()
              .get(spreadsheetId=SPREADSHEET_ID, range=SHEET).execute()).get("values", [])
    if not values:
        raise SystemExit("the YouTube sheet came back empty")

    header, rows = values[0], values[1:]
    idx = {name: i for i, name in enumerate(header)}
    for col in ("video_id", "published_at", "views"):
        if col not in idx:
            raise SystemExit("the sheet has no '%s' column — header changed?" % col)

    out = []
    now = datetime.now(timezone.utc)
    for r in rows:
        def cell(name):
            i = idx[name]
            return r[i] if i < len(r) else ""
        vid, pub, views = cell("video_id").strip(), cell("published_at").strip(), cell("views")
        if not vid or not pub:
            continue
        try:
            published = datetime.strptime(pub[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            stored = int(float(str(views).replace(",", "") or 0))
        except ValueError:
            continue
        if stored <= 0:
            continue
        out.append(dict(video_id=vid, published=published, stored=stored,
                        age=(now - published).days,
                        type=(cell("video_type") if "video_type" in idx else ""),
                        title=(cell("title") if "title" in idx else "")))
    return out


def fetch_live(video_ids, api_key):
    """Current view counts, 50 ids per call — 1 quota unit each."""
    live = {}
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        resp = requests.get(API, params=dict(part="statistics", id=",".join(chunk),
                                             key=api_key), timeout=30)
        if resp.status_code != 200:
            print("   ⚠️ API %s on batch %d: %s" % (resp.status_code, i // 50 + 1,
                                                    resp.text[:160]))
            continue
        for item in resp.json().get("items", []):
            vc = item.get("statistics", {}).get("viewCount")
            if vc is not None:
                live[item["id"]] = int(vc)
    return live


def summarise(label, rows):
    if not rows:
        print("  %-22s (אין פריטים)" % label)
        return
    gains = [r["gain_pct"] for r in rows]
    stored = sum(r["stored"] for r in rows)
    now = sum(r["live"] for r in rows)
    print("  %-22s %4d סרטונים | חציון %+6.1f%% | ממוצע משוקלל %+6.1f%% | %s -> %s"
          % (label, len(rows), statistics.median(gains),
             (now / stored - 1) * 100 if stored else 0,
             format(stored, ","), format(now, ",")))


def main():
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        raise SystemExit("❌ missing YOUTUBE_API_KEY")

    print("=" * 62)
    print("🔎 YouTube 30-day window — is it cutting videos mid-climb?")
    print("=" * 62)

    rows = fetch_sheet()
    print("\n📄 %d videos in the sheet, aged %d-%d days"
          % (len(rows), min(r["age"] for r in rows), max(r["age"] for r in rows)))

    interesting = [r for r in rows if r["age"] >= FROZEN_AFTER or r["age"] <= CONTROL_MAX_AGE]
    print("   asking the API for %d of them (%d calls)"
          % (len(interesting), (len(interesting) + 49) // 50))

    live = fetch_live([r["video_id"] for r in interesting], api_key)
    print("   %d answered (the rest are deleted, private or age-restricted)" % len(live))

    for r in interesting:
        if r["video_id"] in live:
            r["live"] = live[r["video_id"]]
            r["gain"] = r["live"] - r["stored"]
            r["gain_pct"] = (r["live"] / r["stored"] - 1) * 100 if r["stored"] else 0

    got = [r for r in interesting if "live" in r]
    control = [r for r in got if r["age"] <= CONTROL_MAX_AGE]
    frozen = [r for r in got if r["age"] >= FROZEN_AFTER]

    print("\n" + "-" * 62)
    print("בקרה — סרטונים שעדיין בתוך החלון (הגיליון אמור להיות מעודכן):")
    summarise("גיל 0-%d ימים" % CONTROL_MAX_AGE, control)
    if control:
        drift = statistics.median(r["gain_pct"] for r in control)
        print("   %s החציון כאן הוא %.1f%% — %s"
              % ("✅" if drift < 3 else "⚠️", drift,
                 "פער של עד יום איסוף, כצפוי" if drift < 3
                 else "גדול מהצפוי; ייתכן שהגיליון מפגר או שהצימוד שגוי"))

    print("\nמה קרה אחרי שהחלון שחרר אותם:")
    for lo, hi in BUCKETS:
        summarise("גיל %d-%s ימים" % (lo, hi if hi < 100000 else "∞"),
                  [r for r in frozen if lo <= r["age"] < hi])

    if frozen:
        stored = sum(r["stored"] for r in frozen)
        now = sum(r["live"] for r in frozen)
        big = [r for r in frozen if r["gain_pct"] >= 10]
        print("\n" + "-" * 62)
        print("סה\"כ על %d סרטונים קפואים: %s -> %s  (%+.1f%%, %s צפיות שהגיליון לא רואה)"
              % (len(frozen), format(stored, ","), format(now, ","),
                 (now / stored - 1) * 100, format(now - stored, ",")))
        print("%d מהם (%.0f%%) צמחו ב-10%% או יותר אחרי הניתוק."
              % (len(big), 100 * len(big) / len(frozen)))
        print("\nהעשרה שהכי צמחו אחרי שהחלון עזב אותם:")
        for r in sorted(frozen, key=lambda x: -x["gain"])[:10]:
            print("   +%-9s %+7.1f%%  גיל %3d  %s -> %-9s  %s"
                  % (format(r["gain"], ","), r["gain_pct"], r["age"],
                     format(r["stored"], ","), format(r["live"], ","), r["title"][:44]))

        shorts = [r for r in frozen if "שורט" in r["type"] or "Short" in r["type"]]
        regular = [r for r in frozen if r not in shorts]
        if shorts and regular:
            print("\nלפי סוג:")
            summarise("שורטס", shorts)
            summarise("רגילים", regular)

    if "--csv" in sys.argv:
        path = sys.argv[sys.argv.index("--csv") + 1]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["video_id", "published", "age_days", "type",
                        "views_in_sheet", "views_now", "gain", "gain_pct", "title"])
            for r in sorted(got, key=lambda x: x["age"]):
                w.writerow([r["video_id"], r["published"].date(), r["age"], r["type"],
                            r["stored"], r["live"], r["gain"], round(r["gain_pct"], 2),
                            r["title"]])
        print("\n📝 %s" % path)

    print("\nקריאה: אם הדלי של 33-45 ימים כבר מראה צמיחה משמעותית, החלון חותך")
    print("סרטונים באמצע העלייה. אם הוא שטוח, 30 יום מספיקים.")


if __name__ == "__main__":
    main()
