#!/usr/bin/env python3
"""מושך את ההיסטוריה שחסרה למצגת 2024–היום. READ-ONLY.

שום כתיבה לגיליונות ולשום מקום אחר — הרשאת הגיליונות כאן היא `.readonly`,
כך שכתיבה בטעות תיכשל ברמת ה-API ולא רק בהסכמה שלנו.

שלושה חלקים בלתי תלויים, כל אחד לקובץ CSV משלו:

    python deck_history_probe.py sheets    # כל גיליונות הנתונים כמו שהם
    python deck_history_probe.py tiktok    # כל היסטוריית @kan_news בטיקטוק
    python deck_history_probe.py youtube   # כל סרטוני הערוץ אי פעם
    python deck_history_probe.py twitter   # כמה אחורה GetXAPI בכלל מגיע
    python deck_history_probe.py all

## למה כל חלק קיים

**sheets** — הפייפליין אוסף מדצמבר 2025, וזה המקור היחיד ליוטיוב ולעוקבים
היומיים. הייצואים הידניים של Business Suite מכסים רק פייסבוק ואינסטגרם.

**tiktok** — הקולקטור רץ בחלון של 7 ימים ועלה רק ב-2026-07-21, אז בגיליון יש
שבועות ספורים. TikHub מדפדף על הפרופיל כולו ללא הגבלת תאריך, ולכן אפשר להשלים
את כל ההיסטוריה בבת אחת. ~$0.001 לקריאה, 20 סרטונים לעמוד.

**youtube** — הקולקטור מסתכל על 30 יום, אבל ה-API חושף את פלייליסט ההעלאות
כולו: כל סרטון שהערוץ העלה אי פעם, עם הצפיות שלו. שימו לב שהצפיות הן *מצטברות
עד היום* ולא צפיות של אותה שנה — לסרטון מ-2024 היו שנתיים לצבור. ההשוואה
התקפה היא "כמה עלה וכמה צבר", לא "כמה נצפה באותה שנה".

**twitter** — כאן זו *מדידה*, לא משיכה. טיימליין של X חסום בסביבות 3,200
ציוצים, ו-@kann_news מצייץ ~40 ביום; אם זה הגבול, נגיע ל~80 יום בלבד ואין
היסטוריה של 2024. הפרוב מדפדף עד שהספק נעצר ומדווח את התאריך הישן ביותר
שהושג — זו התשובה לשאלה אם טוויטר בכלל יכול להיכנס למצגת רב-שנתית.
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone

import requests

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"

DATA_SHEETS = [
    "נתוני יוטיוב", "נתוני פייסבוק", "נתוני אינסטגרם", "נתוני טיקטוק",
    "נתוני טוויטר", "מעקב עוקבים", "סטוריז אינסטגרם",
]

TIKHUB_BASE = "https://api.tikhub.io"
GETXAPI_BASE = "https://api.getxapi.com"
TIKTOK_USER = os.environ.get("TIKTOK_USERNAME", "kan_news")
TWITTER_USER = os.environ.get("TWITTER_USERNAME", "kann_news")
YOUTUBE_CHANNEL_ID = "UC_HwfTAcjBESKZRJq6BTCpg"

# תקרות ביטחון בלבד. הכיסוי נקבע לפי מה שהספק מפסיק להחזיר, לא לפי אלה —
# ומודפס בסוף כדי שנדע אם נעצרנו בתקרה (כיסוי חלקי) או בסוף הפיד.
# 400 נגמרו ב-3,993 סרטונים מתוך 4,950 שהפרופיל מדווח (ריצה 31498296544),
# כלומר הפיד המשיך. 700 נותן מרווח מעל המספר המדווח.
TIKTOK_MAX_PAGES = int(os.environ.get("TIKTOK_MAX_PAGES", "700"))
TWITTER_MAX_PAGES = int(os.environ.get("TWITTER_MAX_PAGES", "250"))


def _get(url, **kw):
    r = requests.get(url, timeout=60, **kw)
    r.raise_for_status()
    return r.json()


# --- גיליונות ---

def dump_sheets(outdir):
    import gspread
    from google.oauth2.service_account import Credentials

    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT") or os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise SystemExit("❌ missing GCP_SERVICE_ACCOUNT")

    # readonly בכוונה: פרוב לא אמור לכתוב, ועדיף שזה ייאכף מבחוץ
    creds = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    sh = gspread.authorize(creds).open_by_key(SPREADSHEET_ID)

    for name in DATA_SHEETS:
        try:
            ws = sh.worksheet(name)
        except Exception as e:
            print("⚠️  %s — לא נמצא (%s)" % (name, str(e)[:60]), flush=True)
            continue
        rows = ws.get_all_values()
        if not rows:
            print("⚠️  %s — ריק" % name, flush=True)
            continue
        # שם הקובץ באנגלית: שמות הגיליונות בעברית, וזה נוסע דרך artifact upload
        safe = {"נתוני יוטיוב": "youtube", "נתוני פייסבוק": "facebook",
                "נתוני אינסטגרם": "instagram", "נתוני טיקטוק": "tiktok",
                "נתוני טוויטר": "twitter", "מעקב עוקבים": "followers",
                "סטוריז אינסטגרם": "ig_stories"}[name]
        path = os.path.join(outdir, "sheet_%s.csv" % safe)
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerows(rows)
        # עמודת התאריך היא הראשונה או השנייה בכל הגיליונות שלנו
        head = rows[0]
        dcol = next((i for i, h in enumerate(head) if h.strip() in ("date", "published_at")), None)
        span = ""
        if dcol is not None:
            vals = sorted(r[dcol] for r in rows[1:] if len(r) > dcol and r[dcol].strip())
            if vals:
                span = "  |  %s → %s" % (vals[0][:10], vals[-1][:10])
        print("✅ %-18s %6d שורות%s" % (safe, len(rows) - 1, span), flush=True)


# --- טיקטוק: כל ההיסטוריה ---

def dump_tiktok(outdir):
    token = os.environ.get("TIKHUB_TOKEN")
    if not token:
        raise SystemExit("❌ missing TIKHUB_TOKEN")
    h = {"Authorization": "Bearer %s" % token}

    prof = _get("%s/api/v1/tiktok/web/fetch_user_profile" % TIKHUB_BASE,
                headers=h, params={"uniqueId": TIKTOK_USER})
    user = prof["data"]["userInfo"]
    sec_uid = user["user"]["secUid"]
    stats = user.get("stats") or {}
    print("@%s — %s עוקבים, %s סרטונים לפי הפרופיל" % (
        TIKTOK_USER, format(stats.get("followerCount", 0), ","),
        format(stats.get("videoCount", 0), ",")), flush=True)

    rows, seen, cursor, stop = [], set(), 0, "max_pages"
    for page in range(TIKTOK_MAX_PAGES):
        data = _get("%s/api/v1/tiktok/app/v3/fetch_user_post_videos" % TIKHUB_BASE,
                    headers=h, params={"sec_user_id": sec_uid, "max_cursor": cursor,
                                       "count": 20, "sort_type": 0})
        payload = data.get("data") or {}
        batch = payload.get("aweme_list")
        if not isinstance(batch, list) or not batch:
            stop = "end_of_feed"
            break
        for v in batch:
            vid = str(v.get("aweme_id") or "")
            # נעוצים חוזרים גם ראשונים וגם במקומם הכרונולוגי
            if not vid or vid in seen:
                continue
            seen.add(vid)
            ts = v.get("create_time")
            if not ts:
                continue
            s = v.get("statistics") or {}
            rows.append({
                "video_id": vid,
                "date": datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d"),
                "type": "Photo" if (v.get("image_infos") or v.get("aweme_type") == 150) else "Video",
                "title": (v.get("desc") or "").replace("\n", " ")[:500],
                "duration_sec": round((v.get("video") or {}).get("duration", 0) / 1000),
                "views": int(s.get("play_count") or 0),
                "likes": int(s.get("digg_count") or 0),
                "comments": int(s.get("comment_count") or 0),
                "shares": int(s.get("share_count") or 0),
                "whatsapp_shares": int(s.get("whatsapp_share_count") or 0),
                "saves": int(s.get("collect_count") or 0),
            })
        if not payload.get("has_more"):
            stop = "end_of_feed"
            break
        cursor = payload.get("max_cursor", 0)
        if page % 20 == 0 and rows:
            print("   עמוד %d — %d סרטונים, הישן ביותר %s" % (
                page + 1, len(rows), min(r["date"] for r in rows)), flush=True)

    _write(os.path.join(outdir, "tiktok_history.csv"), rows)
    _report("טיקטוק", rows, stop, TIKTOK_MAX_PAGES,
            expected=stats.get("videoCount") or None)


# --- יוטיוב: כל הסרטונים אי פעם ---

def dump_youtube(outdir):
    key = os.environ.get("YOUTUBE_API_KEY")
    if not key:
        raise SystemExit("❌ missing YOUTUBE_API_KEY")
    api = "https://www.googleapis.com/youtube/v3"

    ch = _get("%s/channels" % api, params={
        "part": "contentDetails,statistics", "id": YOUTUBE_CHANNEL_ID, "key": key})
    item = (ch.get("items") or [None])[0]
    if not item:
        raise SystemExit("❌ הערוץ לא נמצא")
    uploads = item["contentDetails"]["relatedPlaylists"]["uploads"]
    st = item.get("statistics") or {}
    # פלייליסט ההעלאות נחתך ב-20,000 פריטים, וזה נראה בדיוק כמו סוף רשימה
    # תקין — ריצה 31498296544 קיבלה 20,000 בול מתוך 51,520 שהערוץ מדווח.
    # מכסה את 2022-08 ואילך, כלומר את כל מה שהמצגת צריכה, אבל לא "כל סרטון".
    reported = int(st.get("videoCount") or 0)
    print("הערוץ: %s מנויים, %s סרטונים, %s צפיות מצטברות" % (
        format(int(st.get("subscriberCount", 0)), ","),
        format(int(st.get("videoCount", 0)), ","),
        format(int(st.get("viewCount", 0)), ",")), flush=True)

    # שלב א' — כל מזהי הסרטונים מפלייליסט ההעלאות
    ids, token = [], None
    while True:
        p = {"part": "contentDetails", "playlistId": uploads, "maxResults": 50, "key": key}
        if token:
            p["pageToken"] = token
        data = _get("%s/playlistItems" % api, params=p)
        ids += [i["contentDetails"]["videoId"] for i in data.get("items", [])]
        token = data.get("nextPageToken")
        if not token:
            break
        if len(ids) % 1000 == 0:
            print("   %s מזהים..." % format(len(ids), ","), flush=True)
    print("   סה\"כ %s מזהי סרטונים" % format(len(ids), ","), flush=True)

    # שלב ב' — סטטיסטיקות, 50 בכל קריאה
    rows = []
    for i in range(0, len(ids), 50):
        data = _get("%s/videos" % api, params={
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(ids[i:i + 50]), "key": key})
        for v in data.get("items", []):
            sn, vs = v.get("snippet") or {}, v.get("statistics") or {}
            dur = (v.get("contentDetails") or {}).get("duration", "")
            rows.append({
                "video_id": v.get("id", ""),
                "date": (sn.get("publishedAt") or "")[:10],
                # Shorts מזוהים לפי אורך, כמו ב-youtube_collector
                "type": "Shorts" if _iso_seconds(dur) <= 60 else "Regular",
                "title": (sn.get("title") or "").replace("\n", " ")[:400],
                "duration_sec": _iso_seconds(dur),
                "views": int(vs.get("viewCount") or 0),
                "likes": int(vs.get("likeCount") or 0),
                "comments": int(vs.get("commentCount") or 0),
            })
        if i and i % 1000 == 0:
            print("   %s סרטונים..." % format(len(rows), ","), flush=True)

    _write(os.path.join(outdir, "youtube_history.csv"), rows)
    _report("יוטיוב", rows, "end_of_feed", 0, expected=reported)


def _iso_seconds(dur):
    """PT1M30S -> 90. אורך ריק נחשב 0, כלומר Shorts — נדיר ולא מזיק."""
    import re
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", dur or "")
    if not m:
        return 0
    h, mi, s = (int(x) if x else 0 for x in m.groups())
    return h * 3600 + mi * 60 + s


# --- טוויטר: עד כמה אחורה אפשר בכלל ---

def dump_twitter(outdir):
    token = os.environ.get("GETXAPI_KEY")
    if not token:
        raise SystemExit("❌ missing GETXAPI_KEY")
    h = {"Authorization": "Bearer %s" % token}

    rows, seen, cursor, stop, empty = [], set(), None, "max_pages", 0
    for page in range(TWITTER_MAX_PAGES):
        # userName, לא username — הספק מחזיר 400 על השני
        params = {"userName": TWITTER_USER}
        if cursor:
            params["cursor"] = cursor
        data = _get("%s/twitter/user/tweets" % GETXAPI_BASE, headers=h, params=params)
        batch = data.get("tweets")
        if not isinstance(batch, list):
            print("❌ עמוד %d — אין רשימת ציוצים בכלל. מפתחות: %s\n    %s" % (
                page + 1, list(data)[:10], str(data)[:300]), flush=True)
            stop = "error"
            break
        if page == 0:
            print("   הספק מדווח %s ציוצים בחשבון" % format(
                data.get("tweet_count") or 0, ","), flush=True)
        if not batch:
            # עמוד ריק *עם* has_more ו-cursor הוא הפרעה זמנית, לא סוף הפיד.
            # להתייחס אליו כסוף זה בדיוק מה ש-twitter_collector נשרף עליו
            # (ריצה 31500308061 כאן עצרה על 0 ציוצים בגלל זה). ממשיכים.
            empty += 1
            print("⚠️  עמוד %d חזר ריק — ממשיכים (has_more=%s)" % (
                page + 1, data.get("has_more")), flush=True)
        for t in batch:
            tid = str(t.get("id") or t.get("tweetId") or t.get("id_str") or "")
            if not tid or tid in seen:
                continue
            seen.add(tid)
            try:
                ts = datetime.strptime(t["createdAt"], "%a %b %d %H:%M:%S %z %Y")
            except (ValueError, KeyError):
                continue
            media = {m.get("type") for m in (t.get("media") or [])}
            rows.append({
                "tweet_id": tid,
                "date": ts.strftime("%Y-%m-%d"),
                "type": "Video" if "video" in media else (
                    "Photo" if media & {"photo", "animated_gif"} else "Text"),
                "text": (t.get("text") or "").replace("\n", " ")[:400],
                "views": int(t.get("viewCount") or t.get("views") or 0),
                "likes": int(t.get("likeCount") or t.get("likes") or 0),
                "replies": int(t.get("replyCount") or 0),
                "retweets": int(t.get("retweetCount") or 0),
                "quotes": int(t.get("quoteCount") or 0),
                "bookmarks": int(t.get("bookmarkCount") or 0),
            })
        if not data.get("has_more") or not data.get("next_cursor"):
            stop = "end_of_feed"
            break
        cursor = data["next_cursor"]
        if page % 20 == 0 and rows:
            print("   עמוד %d — %d ציוצים, הישן ביותר %s" % (
                page + 1, len(rows), min(r["date"] for r in rows)), flush=True)

    _write(os.path.join(outdir, "twitter_history.csv"), rows)
    if empty:
        print("(%d עמודים חזרו ריקים בדרך)" % empty, flush=True)
    _report("טוויטר", rows, stop, TWITTER_MAX_PAGES)


def _write(path, rows):
    if not rows:
        print("⚠️  אין שורות — לא נכתב קובץ", flush=True)
        return
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _report(label, rows, stop, cap, expected=None):
    """`expected` = כמה פריטים החשבון עצמו מדווח שיש לו.

    בלעדיו "הפיד נגמר" זו הצהרה שאי אפשר לבדוק: ספק שנחנק באמצע נראה בדיוק
    כמו סוף פיד תקין. עם המספר המדווח אפשר לומר את ההבדל.
    """
    if not rows:
        print("❌ %s — 0 שורות (stop=%s)" % (label, stop), flush=True)
        return
    oldest, newest = min(r["date"] for r in rows), max(r["date"] for r in rows)
    print("\n%s: %s פריטים  |  %s → %s  |  stop=%s" % (
        label, format(len(rows), ","), oldest, newest, stop), flush=True)
    by_year = {}
    for r in rows:
        y = r["date"][:4]
        by_year[y] = by_year.get(y, 0) + 1
    for y in sorted(by_year):
        print("   %s: %s" % (y, format(by_year[y], ",")), flush=True)

    if stop == "max_pages":
        print("⚠️  נעצרנו בתקרת %d עמודים — הכיסוי חלקי, יש עוד היסטוריה מעבר." % cap,
              flush=True)
    elif expected and len(rows) < expected * 0.98:
        print("⚠️  הפיד נעצר על %s פריטים אבל החשבון מדווח %s — חסרים %s, "
              "וההיסטוריה נחתכת ב-%s ולא מסתיימת בו." % (
                  format(len(rows), ","), format(expected, ","),
                  format(expected - len(rows), ","), oldest), flush=True)
    else:
        print("✅ הפיד נגמר מעצמו — %s הוא באמת הפריט הישן ביותר שהספק חושף." % oldest,
              flush=True)


def main():
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    outdir = os.environ.get("PROBE_OUT", "deck_history")
    os.makedirs(outdir, exist_ok=True)

    jobs = {"sheets": dump_sheets, "tiktok": dump_tiktok,
            "youtube": dump_youtube, "twitter": dump_twitter}
    todo = list(jobs) if what == "all" else [what]
    for name in todo:
        print("\n" + "=" * 62, flush=True)
        print("== %s" % name, flush=True)
        print("=" * 62, flush=True)
        try:
            jobs[name](outdir)
        except KeyError:
            raise SystemExit("❌ unknown target %r (sheets|tiktok|youtube|twitter|all)" % name)
        except Exception as e:
            # חלק אחד שנופל לא אמור להפיל את השניים האחרים
            print("❌ %s נכשל: %s" % (name, str(e)[:200]), flush=True)


if __name__ == "__main__":
    main()
