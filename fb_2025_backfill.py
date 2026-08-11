#!/usr/bin/env python3
"""מושך את מדדי פייסבוק 2025 ברמת פוסט מה-Graph API. READ-ONLY.

הפער האחרון במצגת 2024→היום: לפייסבוק יש ייצוא Business Suite מלא ל-2024
ול-2026, אבל 2025 נתקע שוב ושוב בייצוא הידני. הפרוב `graph_recovery_probe.py`
הראה שה-API הוא מקור נאמן — על 2026, שיש בו את שני המקורות, יחס API/ייצוא
חציוני הוא **1.00 בצפיות** ו-0.97 בחשיפה, על אותם post id.

    python fb_2025_backfill.py --year 2025 --out fb_2025.csv

שום כתיבה לגיליונות. רק GET, ורק לקובץ CSV.

## מה נמשך ומה לא — בכוונה

צפיות, ריאקציות, תגובות, שיתופים וזמן צפייה. **חשיפה לא נמשכת.** אותו פרוב
מצא ש-`post_total_media_view_unique` נשחק עם גיל הפוסט: חציון 12,542 מול
111,098 צפיות במאי 2025 (יחס 0.11), בזמן שב-2026 היחס 0.75, ובמרץ 2024 הוא
מחזיר 106 במקום ה-9,554 שהייצוא של אותו חודש מראה. עמודה שנראית תקינה ואינה
תקינה מסוכנת יותר מעמודה חסרה, ולכן היא לא כאן. חשיפה ל-2025 תגיע מייצוא
Business Suite אם וכאשר.

ריאקציות/תגובות/שיתופים מגיעים כ-summary על אובייקט הפוסט ולא מ-insights,
ולכן הם יציבים ולא נשחקים.
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone

import requests
import pytz

API = "v25.0"
GRAPH = "https://graph.facebook.com/%s" % API
PAGE = os.environ.get("FACEBOOK_PAGE_ID") or "220634478361516"
TOKEN = os.environ.get("FACEBOOK_TOKEN")
IL = pytz.timezone("Asia/Jerusalem")


def get_json(url, params=None, retries=4):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=45)
            data = r.json()
            if "error" in data:
                code = data["error"].get("code")
                # 4/17/32 = rate limit. לחכות ולנסות שוב, לא ליפול.
                if code in (4, 17, 32, 613):
                    print("   ⏳ rate limit (code %s) — ממתין 60ש'" % code, flush=True)
                    time.sleep(60)
                    continue
                return {"__error": data["error"].get("message", "")[:120]}
            return data
        except (requests.RequestException, ValueError):
            if i == retries - 1:
                return {"__error": "retries exhausted"}
            time.sleep(5)
    return {"__error": "retries exhausted"}


def detect_fb_type(post):
    """זהה ל-facebook_collector.detect_media_type."""
    permalink = post.get("permalink_url", "")
    if "/reel/" in permalink:
        return "Reel"
    if "/videos/" in permalink:
        return "Video"
    att_data = (post.get("attachments") or {}).get("data") or []
    if att_data:
        att = att_data[0]
        att_type = att.get("type", "")
        url = att.get("url", "")
        if "reel" in url or "reel" in (att.get("target") or {}).get("url", ""):
            return "Reel"
        if att_type in ["video_inline", "video_direct", "video_autoplay", "video"]:
            return "Video"
        if att_type in ["photo", "cover_photo", "album"]:
            return "Photo"
        if att_type in ["share", "link"]:
            return "Link"
    return "Status"


def video_id_of(post):
    """מזהה אובייקט הווידאו, לזמן הצפייה. אותה שליפה כמו בקולקטור."""
    for att in ((post.get("attachments") or {}).get("data") or []):
        target = att.get("target") or {}
        if att.get("type", "").startswith("video") or "/videos/" in target.get("url", ""):
            tid = target.get("id")
            if tid:
                return tid
    return None


def list_posts(since, until):
    """עמוד אחר עמוד. ה-summary של ריאקציות/תגובות/שיתופים מגיע כאן,
    כלומר בלי קריאה נוספת לפוסט."""
    rows = []
    url = "%s/%s/published_posts" % (GRAPH, PAGE)
    params = {
        "access_token": TOKEN, "limit": 100,
        "fields": ("id,created_time,permalink_url,attachments{type,url,target},"
                   "shares,comments.summary(true).limit(0),"
                   "reactions.summary(true).limit(0)"),
        "since": since, "until": until,
    }
    page = 0
    while url:
        data = get_json(url, params)
        params = None                      # ה-next נושא את הכל
        if data.get("__error"):
            print("   ❌ %s" % data["__error"], flush=True)
            break
        batch = data.get("data", [])
        if not batch:
            break
        for p in batch:
            ct = datetime.strptime(p["created_time"], "%Y-%m-%dT%H:%M:%S%z").astimezone(IL)
            rows.append({
                "post_id": p["id"],
                "date": ct.strftime("%Y-%m-%d"),
                "time": ct.strftime("%H:%M"),
                "type": detect_fb_type(p),
                "permalink": p.get("permalink_url", ""),
                "reactions": ((p.get("reactions") or {}).get("summary") or {}).get("total_count", 0),
                "comments": ((p.get("comments") or {}).get("summary") or {}).get("total_count", 0),
                "shares": (p.get("shares") or {}).get("count", 0),
                "_video_id": video_id_of(p),
            })
        page += 1
        print("   עמוד %d — %d פוסטים (הישן %s)" % (
            page, len(rows), batch[-1].get("created_time", "?")[:10]), flush=True)
        url = (data.get("paging") or {}).get("next")
    return rows


def one_metric(obj_id, metric, endpoint="insights"):
    data = get_json("%s/%s/%s" % (GRAPH, obj_id, endpoint), {"access_token": TOKEN,
                                                             "metric": metric})
    if data.get("__error"):
        return None
    for block in data.get("data", []) or []:
        for v in block.get("values", []) or []:
            val = v.get("value")
            if isinstance(val, dict):
                val = sum(x for x in val.values() if isinstance(x, (int, float)))
            return val
    return None


def enrich(rows):
    n = len(rows)
    t0 = time.time()
    for i, r in enumerate(rows, 1):
        r["views"] = one_metric(r["post_id"], "post_media_view") or 0
        vid = r.pop("_video_id", None)
        if vid:
            # ms -> דקות, כמו בקולקטור
            ms = one_metric(vid, "post_video_view_time", endpoint="video_insights")
            r["watch_min"] = round((ms or 0) / 60000, 1)
            r["video_views"] = one_metric(r["post_id"], "post_video_views") or 0
        else:
            r["watch_min"] = 0
            r["video_views"] = 0
        if i % 100 == 0 or i == n:
            rate = i / max(time.time() - t0, 1)
            print("   %d/%d  (%.1f/ש', נותרו ~%d דק')" % (
                i, n, rate, (n - i) / max(rate, 0.01) / 60), flush=True)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--out", default="fb_backfill.csv")
    ap.add_argument("--limit", type=int, default=0, help="לבדיקה: לעצור אחרי N פוסטים")
    a = ap.parse_args()

    if not TOKEN:
        raise SystemExit("❌ missing FACEBOOK_TOKEN")

    since = int(datetime(a.year, 1, 1, tzinfo=timezone.utc).timestamp())
    until = int(datetime(a.year + 1, 1, 1, tzinfo=timezone.utc).timestamp())

    print("=" * 66)
    print("פייסבוק %d — משיכת מדדים ברמת פוסט" % a.year)
    print("=" * 66)

    print("\nשלב 1: רשימת הפוסטים")
    rows = list_posts(since, until)
    rows = [r for r in rows if r["date"][:4] == str(a.year)]
    print("   סה\"כ %d פוסטים ב-%d" % (len(rows), a.year))
    if a.limit:
        rows = rows[:a.limit]
        print("   מוגבל ל-%d לבדיקה" % len(rows))
    if not rows:
        raise SystemExit("❌ לא נמצאו פוסטים")

    print("\nשלב 2: מדדים לכל פוסט")
    enrich(rows)

    cols = ["post_id", "date", "time", "type", "views", "video_views",
            "watch_min", "reactions", "comments", "shares", "permalink"]
    with open(a.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})

    tot_v = sum(r["views"] for r in rows)
    got = sum(1 for r in rows if r["views"])
    print("\n" + "=" * 66)
    print("נכתב %s — %d שורות" % (a.out, len(rows)))
    print("   צפיות: %s  (%d/%d פוסטים החזירו ערך)" % (format(tot_v, ","), got, len(rows)))
    print("   שעות צפייה: %s" % format(int(sum(r["watch_min"] for r in rows) / 60), ","))
    print("   ריאקציות: %s | תגובות: %s | שיתופים: %s" % (
        format(sum(r["reactions"] for r in rows), ","),
        format(sum(r["comments"] for r in rows), ","),
        format(sum(r["shares"] for r in rows), ",")))
    if got < len(rows) * 0.9:
        print("⚠️  פחות מ-90%% מהפוסטים החזירו צפיות — לבדוק לפני שימוש.")


if __name__ == "__main__":
    sys.exit(main())
