# -*- coding: utf-8 -*-
"""
Instagram caption probe - הכיתוב המלא, בלי הקיטום של הקולקטור.

למה זה קיים: `instagram_collector.py:306` שומר `caption[:500]`. זה בסדר גמור
לדשבורד, אבל הופך חיפוש מילולי בגיליון ללא-אמין - 28% מהפוסטים בחלון המונדיאל
יושבים בדיוק על 500 תווים, כלומר נקטעו, וכל מילה שמופיעה אחרי התו ה-500 פשוט
לא קיימת מבחינת מי שמחפש בגיליון. באינסטגרם זה נפוץ במיוחד: הכיתוב ארוך,
והקרדיטים והתיוגים יושבים בסוף.

בניגוד לטוויטר, כאן אין חור בכיסוי - הגיליון מתחיל 2025-11-21 ומכסה את כל
הטורניר. הבעיה היא רוחב הכיתוב, לא טווח התאריכים, והפתרון הוא Graph API
הרשמי: אותם פוסטים בדיוק, עם הכיתוב השלם.

קריאה בלבד: לא נוגע בגוגל שיטס, לא מבקש insights, לא כותב שום דבר חוץ מ-CSV.

    python instagram_caption_probe.py 2026-06-08 2026-08-11
"""

import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API_VERSION = "v25.0"
BASE = "https://graph.facebook.com/%s/" % API_VERSION
PAGE_ID = os.environ.get("FACEBOOK_PAGE_ID", "220634478361516")
OUTDIR = "ig_captions"

PAGE_SIZE = 50
MAX_PAGES = int(os.environ.get("MAX_PAGES", "120"))
RETRIES = 3
RETRY_SLEEP = 8

IL = timezone(timedelta(hours=3))   # אזור ישראל בקיץ, כמו שהקולקטור שומר


def _token():
    t = os.environ.get("FACEBOOK_TOKEN", "")
    if not t:
        raise SystemExit("❌ missing FACEBOOK_TOKEN")
    return t


def _get(path, **params):
    params["access_token"] = _token()
    return _fetch(BASE + path + "?" + urllib.parse.urlencode(params))


def _fetch(url):
    """URL מלא. עמודי ההמשך של Graph מגיעים ככה, וגם הם צריכים את אותם ניסיונות חוזרים."""
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return json.load(r)
        except Exception as e:                                   # noqa: BLE001
            last = e
            body = ""
            if hasattr(e, "read"):
                try:
                    body = e.read().decode("utf-8", "ignore")[:300]
                except Exception:                                # noqa: BLE001
                    pass
            # טוקן פג הוא לא תקלה חולפת - אין טעם לנסות שוב
            if "OAuthException" in body or "code\":190" in body:
                raise SystemExit("❌ Meta token rejected: %s" % body)
            print("   ⚠️  ניסיון %d/%d: %s %s" % (attempt, RETRIES, str(e)[:80], body[:120]), flush=True)
            if attempt < RETRIES:
                time.sleep(RETRY_SLEEP)
    raise SystemExit("❌ Graph API לא זמין: %s" % last)


def ig_account_id():
    me = _get("me", fields="id,name,instagram_business_account")
    acct = me.get("instagram_business_account")
    if not acct:
        page = _get(PAGE_ID, fields="instagram_business_account,name")
        acct = page.get("instagram_business_account")
        me = page
    if not acct:
        raise SystemExit("❌ לא נמצא חשבון אינסטגרם מקושר")
    print("📷 %s → IG %s" % (me.get("name", "?"), acct["id"]), flush=True)
    return acct["id"]


def content_type(media):
    """זהה למיפוי ב-instagram_collector כדי שאפשר יהיה להצליב לפי סוג."""
    mt = media.get("media_type")
    if mt == "VIDEO":
        return "Reel"
    if mt == "CAROUSEL_ALBUM":
        return "Carousel"
    return "Photo"


def walk(ig_id, start, end):
    """
    כל המדיה שפורסמה בין start ל-end ועד. מחזיר (rows, stop_reason).

    ה-API מחזיר מהחדש לישן, ולכן עוצרים כשפריט נוחת לפני תחילת החלון - זה
    הסימן היחיד שכיסינו את כל הטווח. `paging.next` שנגמר לפני כן הוא כיסוי
    חלקי, ומודפס ככזה במקום להיבלע.
    """
    fields = ("id,caption,media_type,media_product_type,permalink,timestamp,"
              "like_count,comments_count")
    rows, stop, pages, nxt = [], "max_pages", 0, None

    for page in range(MAX_PAGES):
        pages = page + 1
        res = _fetch(nxt) if nxt else _get("%s/media" % ig_id, fields=fields, limit=PAGE_SIZE)
        data = res.get("data")
        if not data:
            stop = "end_of_feed"
            break

        reached = False
        for m in data:
            ts = m.get("timestamp")
            if not ts:
                continue
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z").astimezone(IL)
            day = dt.strftime("%Y-%m-%d")
            if day < start:
                reached = True
                continue
            if day > end:
                continue
            cap = (m.get("caption") or "").replace("\n", " ")
            rows.append({
                "media_id": m["id"],
                "date": day,
                "time": dt.strftime("%H:%M"),
                "type": content_type(m),
                "product_type": m.get("media_product_type", ""),
                "caption_full": cap,
                "caption_len": len(cap),
                "likes": m.get("like_count", 0),
                "comments": m.get("comments_count", 0),
                "permalink": m.get("permalink", ""),
            })
        if reached:
            stop = "reached_window_start"
            break
        nxt = (res.get("paging") or {}).get("next")
        if not nxt:
            stop = "end_of_feed"
            break

    print("   (%d עמודים, stop=%s)" % (pages, stop), flush=True)
    return rows, stop


def main(start, end):
    os.makedirs(OUTDIR, exist_ok=True)
    print("🔎 Graph API captions  %s → %s" % (start, end), flush=True)
    rows, stop = walk(ig_account_id(), start, end)
    rows.sort(key=lambda r: (r["date"], r["time"]))

    path = os.path.join(OUTDIR, "captions_%s_%s.csv" % (start, end))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["media_id"])
        w.writeheader()
        w.writerows(rows)

    over = [r for r in rows if r["caption_len"] > 500]
    print("\n📊 %d פוסטים, %s → %s" % (
        len(rows), rows[0]["date"] if rows else "-", rows[-1]["date"] if rows else "-"), flush=True)
    print("   %d מהם עם כיתוב ארוך מ-500 תווים — אלה שהגיליון קטע" % len(over), flush=True)
    if over:
        longest = max(over, key=lambda r: r["caption_len"])
        print("   הארוך ביותר: %d תווים (%s)" % (longest["caption_len"], longest["date"]), flush=True)
    print("   נשמר: %s" % path, flush=True)
    if stop != "reached_window_start":
        # אף פעם לא להסיק כיסוי מהפריט הישן ביותר - רק מסיבת העצירה
        print("⚠️  העצירה הייתה '%s' ולא 'reached_window_start' — הכיסוי חלקי!" % stop, flush=True)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: python instagram_caption_probe.py <start YYYY-MM-DD> <end YYYY-MM-DD>")
    main(sys.argv[1], sys.argv[2])
