"""
Competitors intraday - רענון תוך-יומי של פיד המתחרים ובדיקת "סיפורים שאין לנו".

רץ ב-11/14/17/20/23 (טיימר ב-VPS -> workflow_dispatch, כמו הסניפר). הריצה היומית
של 08:30 לא משתנה, והיא לבדה כותבת את "מתחרים" (צילום העוקבים: אחד ביום, כי
השינוי היומי מחושב מול השורה הקודמת). כאן נכתבות רק:
  * "מועמדי פערים" - שורת סימון לכל ריצה ושורה לכל סיפור, 30 יום, כדי למדוד
    דיוק לפני שמדליקים טלגרם;
  * "פוסטים מתחרים" - אותו מיזוג לפי post_id של save_posts. ספירות טריות כל
    3 שעות: כשהפיד נמשך פעם ביום, 68% מהפוסטים של ynet קפאו לפני גיל 24 שעות;
  * "היסטוריית פוסטים מתחרים" - שורה לכל פוסט בכל ריצה, 7 ימים, לשלב 3.
לשוניות של כאן לא נכתבות לעולם: שדות ה-_delta שם הם הפרש מול המשיכה היומית.

בדיקת "אין לנו": הגיליון של כאן מעודכן רק עד הבוקר, אז פוסטי אינסטגרם ופייסבוק
של כאן נמשכים כאן חיים (טקסט ותאריך בלבד, בזיכרון) ומצטרפים לגיליון.

הסדר מכוון: מועמדים לפני פוסטים. אם כתיבת המועמדים נכשלת לא נוגעים בפוסטים,
כך שהעמוד לא מציג ספירות מתחרים מאוחרות מבדיקת הסיקור שלו.

Env: FACEBOOK_TOKEN, FACEBOOK_PAGE_ID, GCP_SERVICE_ACCOUNT.
     DRY_RUN=1 - קורא ומחשב ומדפיס, בלי לכתוב.
"""

import os
import sys
import time
from datetime import datetime, timedelta

import gspread
import pandas as pd

import competitors_collector as CC
from utils import http_get_json

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "social_dashboard"))
import competitors_page as CP  # noqa: E402

HISTORY_SHEET = "היסטוריית פוסטים מתחרים"
CANDIDATES_SHEET = "מועמדי פערים"
HISTORY_KEEP_DAYS = 7
CANDIDATES_KEEP_DAYS = 30
HISTORY_COLUMNS = ["post_id", "username", "posted_at", "pulled_at", "age_h", "likes", "comments"]
# הלשוניות היחידות שהריצה רשאית לכתוב
WRITTEN_TABS = (CANDIDATES_SHEET, CC.POSTS_SHEET, HISTORY_SHEET)
# לשוניות של כאן - קריאה בלבד, לבדיקת הכיסוי
OUR_TABS = {"instagram": "נתוני אינסטגרם", "facebook": "נתוני פייסבוק",
            "youtube": "נתוני יוטיוב", "twitter": "נתוני טוויטר", "tiktok": "נתוני טיקטוק"}
SOURCES_CHECKED = "ig:live,fb:live,yt:sheet,x:sheet,tt:sheet"
PAGE_ID = os.environ.get("FACEBOOK_PAGE_ID") or "220634478361516"   # or, not a default: the secret is EMPTY and the workflow sets it anyway
DRY_RUN = os.environ.get("DRY_RUN", "").strip() not in ("", "0")


def history_rows(post_rows, pulled_at):
    """שורה לכל פוסט שנמשך: הספירה וגיל הפוסט ברגע המשיכה."""
    pulled = datetime.strptime(pulled_at, "%Y-%m-%d %H:%M")
    out = []
    for p in post_rows:
        posted_at = f"{p['date']} {p['time']}"
        posted = datetime.strptime(posted_at, "%Y-%m-%d %H:%M")
        out.append({"post_id": str(p["post_id"]), "username": p["username"],
                    "posted_at": posted_at, "pulled_at": pulled_at,
                    "age_h": round((pulled - posted).total_seconds() / 3600, 1),
                    "likes": int(p["likes"]), "comments": int(p["comments"])})
    return out


def _il_date(ts):
    try:
        return (datetime.fromisoformat(str(ts).replace("Z", "+00:00").replace("+0000", "+00:00"))
                .astimezone(CC.IL_TZ).strftime("%Y-%m-%d"))
    except ValueError:
        return ""


def kan_fresh_rows(ig_media, fb_posts):
    """פוסטי כאן מה-API בצורת שורות הגיליון - רק מה ש-our_token_sets קורא."""
    return {
        "instagram": [{"date": _il_date(m.get("timestamp")), "caption": m.get("caption") or ""}
                      for m in ig_media],
        "facebook": [{"date": _il_date(p.get("created_time")), "title": p.get("message") or ""}
                     for p in fb_posts],
    }


def fetch_kan_posts(own_ig):
    """50 האחרונים של כאן באינסטגרם ובפייסבוק: טקסט ותאריך, קריאה אחת לכל פלטפורמה.
    25 לא הספיקו: יום חדשות כבד עובר 25 פוסטי פייסבוק בין 08:30 ל-23:05."""
    ig = http_get_json(f"{CC.BASE}/{own_ig}/media", params={
        "access_token": CC.ACCESS_TOKEN, "fields": "caption,timestamp", "limit": 50})
    fb = http_get_json(f"{CC.BASE}/{PAGE_ID}/published_posts", params={
        "access_token": CC.ACCESS_TOKEN, "fields": "message,created_time", "limit": 50})
    for name, res in (("instagram", ig), ("facebook", fb)):
        if "error" in res:
            raise RuntimeError(f"Kan {name}: {res['error'].get('message', '')[:120]}")
    return ig.get("data", []), fb.get("data", [])


def read_tab(sh, name):
    """שורות לפי כותרת, כמו gsheets בדשבורד; לשונית שאינה קיימת = []."""
    try:
        values = sh.worksheet(name).get_all_values()
    except gspread.WorksheetNotFound:
        return []
    if len(values) < 2:
        return []
    head = [h.strip() for h in values[0]]
    return [{h: (row[i] if i < len(row) else "") for i, h in enumerate(head) if h}
            for row in values[1:]]


def append_log(sh, name, new_rows, columns, date_col, keep_days, now):
    """מוסיף שורות ללשונית יומן בלבד (never clear+rewrite — spec §2.3): כשל
    באמצע לא מוחק היסטוריה, ועמודה שנוספה לגיליון בעבודת יד לא נמחקת ולא
    זזה, כי השורות נכתבות לפי הכותרת שכבר בלשונית ולא לפי `columns`."""
    if name not in WRITTEN_TABS:
        raise ValueError(f"refusing to write {name!r}: the intraday run writes only {WRITTEN_TABS}")
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=len(new_rows) + 200, cols=len(columns))
        ws.update([columns])

    header = ws.row_values(1)
    values = [[r.get(h, "") for h in header] for r in new_rows]
    if values:
        ws.append_rows(values, value_input_option="RAW", insert_data_option="INSERT_ROWS")

    cutoff = (now - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    dates = ws.col_values(header.index(date_col) + 1)[1:]
    k = 0
    for d in dates:
        if d[:10] < cutoff:
            k += 1
        else:
            break
    if k:
        ws.delete_rows(2, k + 1)


def main():
    now = datetime.now(CC.IL_TZ)
    run_at = now.strftime("%Y-%m-%d %H:%M")
    print(f"\n{'=' * 50}\n🥊 Competitors intraday - {run_at}{' (DRY RUN)' if DRY_RUN else ''}\n{'=' * 50}\n")

    if not CC.ACCESS_TOKEN:
        print("❌ Missing FACEBOOK_TOKEN")
        sys.exit(1)
    own_ig = CC.get_own_ig_id()
    if not own_ig:
        print("❌ Could not resolve own IG account id")
        sys.exit(1)

    post_rows, fetched = [], 0
    for username in CC.COMPETITORS:
        snapshot, rows = CC.fetch_account(own_ig, username)   # צילום העוקבים נזרק: פעם ביום בלבד
        if snapshot:
            fetched += 1
            post_rows.extend(rows)
        time.sleep(0.3)
    print(f"📥 {fetched}/{len(CC.COMPETITORS)} accounts, {len(post_rows)} posts")
    if not fetched:
        print("❌ No accounts fetched")
        sys.exit(1)

    ig_media, fb_posts = fetch_kan_posts(own_ig)
    print(f"📥 Kan live: {len(ig_media)} IG, {len(fb_posts)} FB")

    sh = CC._open()
    data = {key: read_tab(sh, tab) for key, tab in OUR_TABS.items()}
    fresh = kan_fresh_rows(ig_media, fb_posts)
    data["instagram"] += fresh["instagram"]
    data["facebook"] += fresh["facebook"]
    merged = {str(p.get("post_id", "")): p for p in read_tab(sh, CC.POSTS_SHEET)}
    merged.update({str(p["post_id"]): p for p in post_rows})
    data["competitor_posts"] = list(merged.values())

    items, checked = CP.gap_clusters(data, now.replace(tzinfo=None))
    cand = CP.candidate_rows(items, run_at, SOURCES_CHECKED)
    missed = sum(1 for g in items if g["kind"] == "missed")
    print(f"🕳 {checked} strong posts checked -> {missed} missed, {len(items) - missed} exclusive")
    for g in items[:5]:
        print(f"   {g['kind']:9} x{g['n_outlets']} {g['lead']['username']}: {g['caption'][:70]}")

    if DRY_RUN:
        print("\n🧪 DRY RUN - nothing written")
        return

    # 1. מועמדים - לפני הפוסטים (ראו הדוקסטרינג)
    append_log(sh, CANDIDATES_SHEET, cand, CP.CANDIDATE_COLUMNS, "run_at", CANDIDATES_KEEP_DAYS, now)
    print(f"✅ {CANDIDATES_SHEET}: +{len(cand)} rows")
    # 2. פוסטים - אותו מיזוג בדיוק כמו בריצה היומית
    CC.save_posts(sh, pd.DataFrame(post_rows))
    # 3. היסטוריה
    hist = history_rows(post_rows, run_at)
    append_log(sh, HISTORY_SHEET, hist, HISTORY_COLUMNS, "pulled_at", HISTORY_KEEP_DAYS, now)
    print(f"✅ {HISTORY_SHEET}: +{len(hist)} rows")


if __name__ == "__main__":
    main()
