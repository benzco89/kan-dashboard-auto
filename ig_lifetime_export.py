"""
ייצוא: הביצועים של כל פוסט אינסטגרם עד היום, לא רק עד סוף השבוע הראשון.

הגיליון קופא 7 ימים אחרי הפרסום (DAYS_BACK בקולקטור), כך שהמספרים שבו הם
ביצועי השבוע הראשון. כאן שואלים את Graph מחדש על כל פוסט בטווח, עם אותן
פונקציות בדיוק שהקולקטור משתמש בהן - אותם מדדים, אותה גרסת API.

קריאה בלבד: לא כותב לגוגל שיטס. הפלט הוא JSON ב-ig_lifetime/ ל-artifact.

    python ig_lifetime_export.py 2026-07-13 2026-10-10
"""
import json
import os
import re
import sys
from datetime import datetime, timezone

import pytz

import instagram_collector as ic
from utils import http_get_json

IL = pytz.timezone("Asia/Jerusalem")
OUT_DIR = "ig_lifetime"


def media_in_range(ig_id, start, end):
    """כל המדיה שפורסמה בין start ל-end (כולל, לפי תאריך ישראל), מהחדש לישן."""
    url = f"https://graph.facebook.com/{ic.API_VERSION}/{ig_id}/media"
    params = {"access_token": ic.ACCESS_TOKEN, "limit": 50,
              "fields": "id,caption,media_type,permalink,timestamp,like_count,comments_count"}
    out = []
    while url:
        res = http_get_json(url, params=params)
        if "error" in res:
            sys.exit(f"❌ media list: {res['error'].get('message')}")
        page = res.get("data") or []
        done = False
        for m in page:
            ts = datetime.fromisoformat(re.sub(r"\+0000$", "+00:00", m["timestamp"].replace("Z", "+00:00")))
            day = ts.astimezone(IL).date().isoformat()
            if day < start:
                done = True
                break
            if day <= end:
                m["_date"], m["_time"] = day, ts.astimezone(IL).strftime("%H:%M")
                out.append(m)
        if done or not page:
            break
        url, params = (res.get("paging") or {}).get("next"), {}
    return out


def main():
    start, end = sys.argv[1], sys.argv[2]
    if not ic.ACCESS_TOKEN:
        sys.exit("❌ missing FACEBOOK_TOKEN")
    os.makedirs(OUT_DIR, exist_ok=True)
    ig_id = ic.get_instagram_account_id()
    if not ig_id:
        sys.exit("❌ no Instagram account id")
    media = media_in_range(ig_id, start, end)
    print(f"📥 {len(media)} posts between {start} and {end}")

    measured_at = datetime.now(IL).strftime("%Y-%m-%d %H:%M")
    rows, empty = [], 0
    for n, m in enumerate(media, 1):
        mtype = m.get("media_type", "IMAGE")
        ins = ic.get_media_insights(m["id"], mtype)
        reel = ic.get_reel_v25_metrics(m["id"]) if mtype in ("VIDEO", "REELS") else {}
        if not ins.get("views"):
            empty += 1
        rows.append({
            "media_id": m["id"], "date": m["_date"], "time": m["_time"], "media_type": mtype,
            "caption": (m.get("caption") or "").replace("\n", " "), "permalink": m.get("permalink", ""),
            "likes": m.get("like_count", 0), "comments": m.get("comments_count", 0),
            **ins, **reel, "measured_at": measured_at,
        })
        if n % 100 == 0:
            print(f"   {n}/{len(media)}")

    with open(os.path.join(OUT_DIR, f"ig_lifetime_{start}_{end}.json"), "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)
    print(f"✅ {len(rows)} posts measured at {measured_at}; {empty} came back with 0 views")
    # אחוז גבוה של אפסים = כשל מדד, לא פוסטים ריקים - להיכשל כדי שזה ייראה
    if rows and empty / len(rows) > 0.05:
        sys.exit(f"❌ {empty}/{len(rows)} posts with 0 views - looks like a metric failure")


if __name__ == "__main__":
    main()
