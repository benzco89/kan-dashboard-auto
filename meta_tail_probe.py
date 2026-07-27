#!/usr/bin/env python3
"""Do Facebook and Instagram posts keep earning after the collector stops looking?

READ-ONLY. No writes to Google Sheets, no collector touched.

What is known: a post is essentially finished by its fourth day. The whole week
of 12-18/07 gained 0.2-0.7% between day four and its freeze. But that measurement
can only see as far as the collector does — both collectors keep a 7-day window,
so **nothing is known about day eight onwards**. It is the same blind spot
YouTube had before `youtube_tail_probe.py`, and there the answer turned out to be
9.1M views.

The sheet cannot answer it: a post older than 7 days stops being refreshed, so
its stored figure is frozen and waiting does not move it. The live number has to
come from the Graph API — which makes the measurement available today, because
the sheets already hold months of posts that froze long ago.

Same shape as the YouTube probe, including the control group: posts still inside
the window should match the sheet almost exactly. If they do not, the probe is
measuring a broken join or a lagging collector, not a tail.

    python meta_tail_probe.py
    python meta_tail_probe.py --csv out.csv

Env: FACEBOOK_TOKEN, GCP_SERVICE_ACCOUNT (or service-account.json).
"""

import os
import sys
import csv
import json
import statistics
from datetime import datetime, timezone

import requests

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
V = "v25.0"
GRAPH = "https://graph.facebook.com/%s" % V

WINDOW_DAYS = 7
FROZEN_AFTER = 10        # a few days past the window, so the value is certainly frozen
CONTROL_MAX_AGE = 5      # still refreshed daily — the control
BUCKETS = [(10, 20), (20, 40), (40, 70), (70, 120), (120, 100000)]

SHEETS = {
    "facebook": dict(name="נתוני פייסבוק", id_col="post_id", date_col="date",
                     views_col="views", metric="post_video_views"),
    "instagram": dict(name="נתוני אינסטגרם", id_col="media_id", date_col="date",
                      views_col="views", metric="views"),
}


def fetch_sheet(sheet_name):
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
            raise SystemExit("❌ no credentials: set GCP_SERVICE_ACCOUNT")
        creds = service_account.Credentials.from_service_account_file(path, scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return (svc.spreadsheets().values()
            .get(spreadsheetId=SPREADSHEET_ID, range=sheet_name).execute()).get("values", [])


def rows_of(platform):
    cfg = SHEETS[platform]
    values = fetch_sheet(cfg["name"])
    if not values:
        return []
    header, body = values[0], values[1:]
    idx = {n: i for i, n in enumerate(header)}
    for col in (cfg["id_col"], cfg["date_col"], cfg["views_col"]):
        if col not in idx:
            raise SystemExit("❌ %s has no '%s' column" % (platform, col))
    now = datetime.now(timezone.utc)
    out = []
    for r in body:
        cell = lambda n: r[idx[n]] if idx[n] < len(r) else ""
        pid, date = str(cell(cfg["id_col"])).strip(), str(cell(cfg["date_col"])).strip()[:10]
        if not pid or not date:
            continue
        try:
            when = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            stored = int(float(str(cell(cfg["views_col"])).replace(",", "") or 0))
        except ValueError:
            continue
        if stored <= 0:
            continue
        out.append(dict(id=pid, date=date, stored=stored, age=(now - when).days,
                        title=str(cell("title") if "title" in idx else
                                  (cell("caption") if "caption" in idx else ""))[:60]))
    return out


def fetch_live(platform, ids, token):
    """Current view count per item. Instagram answers per-media insights; Facebook
    keeps video views on the post's own insights edge."""
    cfg = SHEETS[platform]
    live = {}
    for i, pid in enumerate(ids, 1):
        if i % 50 == 0:
            print("      %d/%d" % (i, len(ids)))
        try:
            r = requests.get("%s/%s/insights" % (GRAPH, pid),
                             params={"access_token": token, "metric": cfg["metric"]},
                             timeout=30)
            data = r.json()
            if "error" in data:
                continue
            for block in data.get("data", []):
                tv = block.get("total_value")
                if isinstance(tv, dict) and tv.get("value") is not None:
                    live[pid] = int(tv["value"])
                    break
                for v in block.get("values", []) or []:
                    if v.get("value") is not None:
                        live[pid] = int(v["value"])
                        break
        except Exception:
            continue
    return live


def summarise(label, rows):
    if not rows:
        print("  %-22s (אין פריטים)" % label)
        return
    gains = [r["gain_pct"] for r in rows]
    stored = sum(r["stored"] for r in rows)
    now = sum(r["live"] for r in rows)
    print("  %-22s %4d פריטים | חציון %+6.1f%% | משוקלל %+6.1f%% | %s -> %s"
          % (label, len(rows), statistics.median(gains),
             (now / stored - 1) * 100 if stored else 0,
             format(stored, ","), format(now, ",")))


def run(platform, token, sample):
    print("\n" + "=" * 66)
    print("  %s" % platform.upper())
    print("=" * 66)
    rows = rows_of(platform)
    if not rows:
        print("  (הגיליון ריק)")
        return []
    print("  %d פריטים בגיליון, בני %d-%d ימים"
          % (len(rows), min(r["age"] for r in rows), max(r["age"] for r in rows)))

    control = [r for r in rows if r["age"] <= CONTROL_MAX_AGE][:sample]
    frozen = [r for r in rows if r["age"] >= FROZEN_AFTER]
    # evenly spread across the age buckets rather than the newest N, or the old
    # end of the curve — the part nobody has ever seen — never gets sampled
    picked = []
    for lo, hi in BUCKETS:
        band = [r for r in frozen if lo <= r["age"] < hi]
        step = max(1, len(band) // max(1, sample // len(BUCKETS)))
        picked.extend(band[::step][:sample // len(BUCKETS)])

    todo = control + picked
    print("  נשאל את ה-API על %d מהם (%d בקרה, %d קפואים)"
          % (len(todo), len(control), len(picked)))
    live = fetch_live(platform, [r["id"] for r in todo], token)
    print("  %d ענו" % len(live))

    for r in todo:
        if r["id"] in live:
            r["live"] = live[r["id"]]
            r["gain"] = r["live"] - r["stored"]
            r["gain_pct"] = (r["live"] / r["stored"] - 1) * 100 if r["stored"] else 0
    got = [r for r in todo if "live" in r]

    print("\n  בקרה — עדיין בתוך החלון (הגיליון אמור להיות מעודכן):")
    ctl = [r for r in got if r["age"] <= CONTROL_MAX_AGE]
    summarise("גיל 0-%d ימים" % CONTROL_MAX_AGE, ctl)
    if ctl:
        drift = statistics.median(r["gain_pct"] for r in ctl)
        print("   %s חציון %.1f%% — %s" % ("✅" if abs(drift) < 3 else "⚠️", drift,
              "כצפוי" if abs(drift) < 3 else "גדול מהצפוי; ייתכן שהשיטה מודדת משהו אחר"))

    print("\n  אחרי שהחלון שחרר אותם:")
    fr = [r for r in got if r["age"] >= FROZEN_AFTER]
    for lo, hi in BUCKETS:
        summarise("גיל %d-%s ימים" % (lo, hi if hi < 100000 else "∞"),
                  [r for r in fr if lo <= r["age"] < hi])
    if fr:
        stored, now = sum(r["stored"] for r in fr), sum(r["live"] for r in fr)
        big = [r for r in fr if r["gain_pct"] >= 10]
        print("\n  סה\"כ על %d קפואים: %s -> %s (%+.1f%%) · %d צמחו מעל 10%%"
              % (len(fr), format(stored, ","), format(now, ","),
                 (now / stored - 1) * 100 if stored else 0, len(big)))
        for r in sorted(fr, key=lambda x: -x.get("gain", 0))[:5]:
            print("     +%-9s %+7.1f%%  גיל %3d  %s" % (format(r["gain"], ","),
                                                        r["gain_pct"], r["age"], r["title"]))
    return got


def main():
    token = os.environ.get("FACEBOOK_TOKEN")
    if not token:
        raise SystemExit("❌ missing FACEBOOK_TOKEN")
    sample = 150
    for i, a in enumerate(sys.argv):
        if a == "--sample" and i + 1 < len(sys.argv):
            sample = int(sys.argv[i + 1])

    print("=" * 66)
    print("🔎 יש זנב לפייסבוק ולאינסטגרם אחרי 7 ימים?")
    print("   %s UTC · דגימה של עד %d פריטים לפלטפורמה"
          % (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), sample))
    print("=" * 66)

    allrows = []
    for platform in ("facebook", "instagram"):
        try:
            allrows += [dict(r, platform=platform) for r in run(platform, token, sample)]
        except SystemExit:
            raise
        except Exception as e:
            print("  ⚠️ %s: %s" % (platform, str(e)[:120]))

    if "--csv" in sys.argv and allrows:
        path = sys.argv[sys.argv.index("--csv") + 1]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["platform", "id", "date", "age_days", "views_in_sheet",
                        "views_now", "gain", "gain_pct", "title"])
            for r in sorted(allrows, key=lambda x: (x["platform"], x["age"])):
                w.writerow([r["platform"], r["id"], r["date"], r["age"], r["stored"],
                            r["live"], r["gain"], round(r["gain_pct"], 2), r["title"]])
        print("\n📝 %s" % path)

    print("\nקריאה: אם הדלי של 10-20 ימים כבר שטוח, החלון של 7 ימים לא מפספס דבר.")
    print("אם הוא צומח כמו ביוטיוב, יש שם צפיות שאיננו סופרים.")


if __name__ == "__main__":
    main()
