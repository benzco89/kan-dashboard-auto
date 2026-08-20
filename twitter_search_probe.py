# -*- coding: utf-8 -*-
"""
Twitter/X history probe - GetXAPI advanced search.

למה זה קיים: הגיליון "נתוני טוויטר" מתחיל ב-2026-06-21, אבל מונדיאל 2026 נפתח
ב-11.6. עשרת הימים הראשונים של הטורניר פשוט לא קיימים אצלנו, ואת החור הזה
*אי אפשר* לסגור עם endpoint הטיימליין: `deck_history_probe.py twitter`
(ריצה 31500547213) דפדף עד תום והגיע 13 יום אחורה בלבד.

הפתרון הוא endpoint אחר. `/twitter/tweet/advanced_search` יושב על אינדקס
החיפוש של X, לא על הטיימליין, ולכן `since:`/`until:` מגיעים אחורה כמה
שצריך. זה גם אומר שזאת לא הכחשה של הממצא הישן - הטיימליין באמת נעצר
אחרי 13 יום; פשוט לא הוא הכלי לשליפה היסטורית.

קריאה בלבד: לא נוגע בגוגל שיטס, לא כותב שום דבר חוץ מ-CSV מקומי.

    python twitter_search_probe.py 2026-06-08 2026-07-21

מבנה העמודות זהה לגיליון, כדי שאפשר יהיה למזג בלי תרגום.
"""

import csv
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

API = "https://api.getxapi.com/twitter/tweet/advanced_search"
USERNAME = os.environ.get("TWITTER_USERNAME", "kann_news")
OUTDIR = "twitter_search"

# פריסה ליום-יום ולא לחלון אחד ארוך. שתי סיבות: ~40 ציוצים ביום זה 2 עמודים,
# כלומר אף פעם לא מדפדפים עמוק (ושם החיפוש של X נוטה להיקטע), וכל יום מקבל
# stop reason משלו - כך שיום קטוע נראה כיום קטוע ולא נבלע בסך-הכל.
MAX_PAGES_PER_DAY = int(os.environ.get("MAX_PAGES_PER_DAY", "12"))
# כמה פעמים מותר לשאול שוב על תחתית אותו יום אחרי קטיעה. ב-13.7 הספיקה אחת.
MAX_SLICES_PER_DAY = int(os.environ.get("MAX_SLICES_PER_DAY", "8"))
# עד כמה קרוב לתחילת החלון צריך להגיע כדי לקרוא ליום שלם. @kann_news מצייץ
# מסביב לשעון, אז פער אמיתי של יותר משעה בתחתית היום הוא קטיעה, לא שקט.
BOUNDARY_TOLERANCE_SEC = int(os.environ.get("BOUNDARY_TOLERANCE_SEC", "3600"))
RETRIES = 3
RETRY_SLEEP = 10

IL_OFFSET = timedelta(hours=3)  # אזור ישראל בקיץ; הגיליון שומר שעון מקומי


def _headers():
    key = os.environ.get("GETXAPI_KEY", "")
    if not key:
        raise SystemExit("❌ missing GETXAPI_KEY")
    return {"Authorization": "Bearer %s" % key}


def _get(params):
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(API, headers=_headers(), params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:                                  # noqa: BLE001
            last = e
            print("   ⚠️  ניסיון %d/%d נכשל: %s" % (attempt, RETRIES, str(e)[:120]), flush=True)
            if attempt < RETRIES:
                time.sleep(RETRY_SLEEP)
    raise SystemExit("❌ GetXAPI לא זמין: %s" % last)


def parse_time(tweet):
    try:
        return datetime.strptime(tweet["createdAt"], "%a %b %d %H:%M:%S %z %Y")
    except (ValueError, KeyError, TypeError):
        return None


def classify_media(tweet):
    """זהה ל-twitter_collector.classify_media - חייב להישאר זהה כדי שהמיזוג יתיישב."""
    media = tweet.get("media") or []
    types = {m.get("type") for m in media}
    if "video" in types:
        return "Video"
    if "photo" in types or "animated_gif" in types:
        return "Photo"
    return "Text"


def tweet_id_of(tweet):
    for key in ("id", "tweetId", "id_str", "rest_id"):
        val = tweet.get(key)
        if val:
            return str(val)
    m = re.search(r"/status/(\d+)", tweet.get("url", "") or "")
    return m.group(1) if m else ""


def _fetch_slice(since_ts, until_ts, seen):
    """
    חלון זמן אחד, בשניות יוניקס. מחזיר (tweets_חדשים, stop_reason).

    שתי מלכודות של GetXAPI, שתיהן כבר עלו כאן: הוא ממשיך להנפיק next_cursor
    אחרי שהפסיק להחזיר משהו חדש (בפרוב ההיסטוריה עמודים 41-241 החזירו את
    אותם 710), והוא עונה 200 על עמוד ריק. לכן עוצרים על "העמוד לא הוסיף
    ציוץ חדש", לא על has_more לבדו.
    """
    q = "from:%s since_time:%d until_time:%d" % (USERNAME, since_ts, until_ts)
    out, cursor, stop = [], None, "max_pages"
    for _ in range(MAX_PAGES_PER_DAY):
        params = {"q": q, "product": "Latest"}
        if cursor:
            params["cursor"] = cursor
        data = _get(params)

        batch = data.get("tweets")
        if not isinstance(batch, list) or not batch:
            stop = "empty_page"
            break

        fresh = 0
        for t in batch:
            tid = tweet_id_of(t)
            if not tid or tid in seen:
                continue
            seen.add(tid)
            out.append(t)
            fresh += 1

        if fresh == 0:                      # הספק מדשדש על אותו עמוד
            stop = "no_progress"
            break
        if not data.get("has_more") or not data.get("next_cursor"):
            stop = "end_of_results"
            break
        cursor = data["next_cursor"]

    return out, stop


def fetch_day(day):
    """
    כל הציוצים של @USERNAME ביום אחד, עם ריפוי-עצמי לקטיעות.

    למה זה לא סתם שאילתה אחת: ב-13.7 החיפוש החזיר את כל הציוצים עד 15:00
    שעון ישראל ואז הפסיק - עם has_more=false, כלומר בדיוק כמו יום שנגמר
    כמו שצריך. 36 מתוך 59 הציוצים של אותו יום נעלמו ככה בשקט, ורק השוואה
    מול הגיליון חשפה את זה. has_more=false הוא לא הוכחה לשלמות.

    התוצאות חוזרות מהחדש לישן, ולכן קטיעה תמיד מותירה חור בתחתית החלון:
    אם הציוץ הישן ביותר שחזר רחוק מתחילת החלון, פשוט שואלים שוב על מה
    שנשאר מתחתיו. זה מרפא כל קטיעה בלי להניח דבר על הסיבה לה.
    """
    d0 = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc) - IL_OFFSET
    start, end = int(d0.timestamp()), int((d0 + timedelta(days=1)).timestamp())

    seen, out, stops, lo = set(), [], [], end
    for depth in range(MAX_SLICES_PER_DAY):
        got, stop = _fetch_slice(start, lo, seen)
        out.extend(got)
        stops.append(stop)
        if not got:
            break
        oldest = min(int(parse_time(t).timestamp()) for t in got if parse_time(t))
        if oldest <= start + BOUNDARY_TOLERANCE_SEC:
            break                            # הגענו לתחתית החלון - היום שלם
        lo = oldest                          # נקטע: לשאול שוב על מה שמתחתיו
    else:
        stops.append("slice_ceiling")

    return out, "+".join(stops[:1]) + ("" if len(stops) == 1 else " ×%d" % len(stops))


def to_row(t):
    created = parse_time(t)
    if not created:
        return None
    il = created.astimezone(timezone(IL_OFFSET))
    views = int(t.get("viewCount") or 0)
    likes = int(t.get("likeCount") or 0)
    rts = int(t.get("retweetCount") or 0)
    reps = int(t.get("replyCount") or 0)
    quotes = int(t.get("quoteCount") or 0)
    total = likes + rts + reps + quotes
    tid = tweet_id_of(t)
    return {
        "tweet_id": tid,
        "date": il.strftime("%Y-%m-%d"),
        "time": il.strftime("%H:%M"),
        "type": classify_media(t),
        "text": (t.get("text", "") or "").replace("\n", " ")[:500],
        "views": views,
        "likes": likes,
        "retweets": rts,
        "replies": reps,
        "quotes": quotes,
        "bookmarks": int(t.get("bookmarkCount") or 0),
        "total_engagement": total,
        "engagement_rate": round((total / views) * 100, 2) if views else 0,
        "permalink": t.get("url") or (
            "https://x.com/%s/status/%s" % (USERNAME, tid) if tid else ""),
    }


def main(start, end):
    os.makedirs(OUTDIR, exist_ok=True)
    d0 = datetime.strptime(start, "%Y-%m-%d")
    d1 = datetime.strptime(end, "%Y-%m-%d")
    if d1 < d0:
        raise SystemExit("❌ תאריך הסיום מוקדם מתאריך ההתחלה")

    print("🔎 advanced_search @%s  %s → %s" % (USERNAME, start, end), flush=True)

    rows, by_day, truncated, calls = [], {}, [], 0
    day = d0
    while day <= d1:
        ds = day.strftime("%Y-%m-%d")
        tweets, stop = fetch_day(ds)
        calls += 1
        got = [r for r in (to_row(t) for t in tweets) if r]
        rows.extend(got)
        by_day[ds] = (len(got), stop)
        if "slice_ceiling" in stop or "max_pages" in stop:
            truncated.append(ds)
        print("   %s  %3d ציוצים  stop=%s" % (ds, len(got), stop), flush=True)
        day += timedelta(days=1)

    # החיפוש מחזיר לפי UTC, אז ציוצים על הגבול נוחתים ביום שכן/מחוץ לטווח.
    # שומרים הכל ומסמנים - עדיף עודף על חור.
    rows.sort(key=lambda r: (r["date"], r["time"]))
    seen, uniq = set(), []
    for r in rows:
        if r["tweet_id"] in seen:
            continue
        seen.add(r["tweet_id"])
        uniq.append(r)

    path = os.path.join(OUTDIR, "tweets_%s_%s.csv" % (start, end))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(uniq[0].keys()) if uniq else ["tweet_id"])
        w.writeheader()
        w.writerows(uniq)

    print("\n📊 %d ציוצים ייחודיים, %s → %s" % (
        len(uniq), uniq[0]["date"] if uniq else "-", uniq[-1]["date"] if uniq else "-"), flush=True)
    print("   נשמר: %s" % path, flush=True)
    if truncated:
        # אף פעם לא להסיק כיסוי מהשורה הישנה ביותר - רק מסיבת העצירה.
        print("⚠️  ימים שנעצרו בתקרת העמודים (כיסוי חלקי!): %s" % ", ".join(truncated), flush=True)
    empty = [d for d, (n, _) in by_day.items() if n == 0]
    if empty:
        print("⚠️  ימים ללא ציוצים כלל: %s" % ", ".join(empty), flush=True)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: python twitter_search_probe.py <YYYY-MM-DD start> <YYYY-MM-DD end>")
    main(sys.argv[1], sys.argv[2])
