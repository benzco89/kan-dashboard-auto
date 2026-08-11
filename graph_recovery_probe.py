#!/usr/bin/env python3
"""האם ה-Graph API מחזיר מדדים שהייצוא של Business Suite כבר לא נותן?

READ-ONLY. מדגם קטן בלבד — 15 פריטים לכל שאלה. שום כתיבה לשום מקום.

    python graph_recovery_probe.py

ארבע שאלות, כל אחת עם השלכה ישירה על המצגת 2024→היום:

1. **פייסבוק 2025 ברמת פוסט.** הפער הגדול שנותר. הייצוא של 2025 נתקע ל-Business
   Suite, ויש לנו רק ספירות. אם ה-API מחזיר `post_media_view` ו-
   `post_total_media_view_unique` לפוסטים בני שנה וחצי — אין צורך בייצוא בכלל.

2. **פייסבוק 2024 ברמת פוסט.** לייצוא של 2024 אין `Views` לפני ספטמבר. אם
   ה-API כן נותן, מרוויחים שמונה חודשים.

3. **אינסטגרם בתחילת 2024 — האם אפשר לשחזר.** זו השאלה היקרה. בייצוא, `Views`
   ריק לפני יולי 2024 ו-`Reach` הוא זבל לפני ספטמבר (חציון 3–24 על פוסטים עם
   1,500 לייקים). אם ה-API מחזיר מספרים שפויים לאותם פריטים בדיוק, חצי שנה
   שנחשבה אבודה חוזרת. הפרוב משווה API מול הייצוא **על אותו media id**, כי רק
   ההשוואה הזו מבדילה בין שחזור לבין מספר אחר שנראה סביר.

4. **תובנות ברמת העמוד אחורה בזמן.** האם `since/until` על `/insights` של העמוד
   מחזיר סדרה יומית מ-2024, או שמטא חותכת גם אותה.

Env: FACEBOOK_TOKEN. קורא את `analysis/yearly_content/` לזיהוי פריטי 2024.
"""

import csv
import os
import random
import sys
from datetime import datetime, timedelta

import requests

V = "v25.0"
GRAPH = "https://graph.facebook.com/%s" % V
PAGE = os.environ.get("FACEBOOK_PAGE_ID") or "220634478361516"
TOKEN = os.environ.get("FACEBOOK_TOKEN")
SAMPLE = 15

HERE = os.path.dirname(os.path.abspath(__file__))
IG_2024 = os.path.join(HERE, "analysis", "yearly_content",
                       "Jan-01-2024_Dec-31-2024_4588672571364735.csv")


def get(path, **params):
    params["access_token"] = TOKEN
    try:
        r = requests.get("%s/%s" % (GRAPH, path), params=params, timeout=40)
        return r.json()
    except Exception as e:
        return {"error": {"message": str(e)}}


def err(res):
    e = (res or {}).get("error")
    return e.get("message", "")[:110] if e else None


def insight(obj_id, metric, endpoint="insights"):
    """ערך יחיד. מחזיר (value, error) — None כערך פירושו שהמדד לא חזר."""
    res = get("%s/%s" % (obj_id, endpoint), metric=metric)
    e = err(res)
    if e:
        return None, e
    for block in res.get("data", []) or []:
        for v in block.get("values", []) or []:
            val = v.get("value")
            if isinstance(val, dict):
                val = sum(x for x in val.values() if isinstance(x, (int, float)))
            return val, None
    return None, "no value"


def fb_posts_in(since, until, limit=SAMPLE):
    res = get("%s/published_posts" % PAGE, fields="id,created_time",
              since=since, until=until, limit=limit)
    if err(res):
        print("   ❌ %s" % err(res))
        return []
    return [(p["id"], p.get("created_time", "")[:10]) for p in res.get("data", [])]


def probe_fb_posts(label, since, until):
    print("\n" + "-" * 70)
    print("%s  (%s → %s)" % (label, since, until))
    posts = fb_posts_in(since, until)
    if not posts:
        print("   אין פוסטים בחלון")
        return
    metrics = ["post_media_view", "post_total_media_view_unique",
               "post_impressions", "post_video_views"]
    got = {m: [] for m in metrics}
    first_err = {}
    for pid, day in posts:
        for m in metrics:
            val, e = insight(pid, m)
            if val is not None:
                got[m].append(val)
            elif e and m not in first_err:
                first_err[m] = e
    print("   נדגמו %d פוסטים" % len(posts))
    for m in metrics:
        vals = got[m]
        if vals:
            vals_sorted = sorted(vals)
            print("   ✅ %-32s %2d/%d חזרו | חציון %s" % (
                m, len(vals), len(posts),
                format(int(vals_sorted[len(vals_sorted) // 2]), ",")))
        else:
            print("   ❌ %-32s 0/%d  | %s" % (m, len(posts),
                                              first_err.get(m, "ריק")))


FB_2026 = os.path.join(HERE, "analysis", "yearly_content",
                       "Jan-01-2026_Aug-11-2026_1369894801784288.csv")


def probe_fb_control():
    """הבקרה: על 2026 יש גם ייצוא וגם API. האם הם מסכימים?

    בלי זה, "15/15 חזרו" ל-2025 לא אומר שהמספרים נכונים — רק שמשהו חזר.
    חשיפת ה-API ל-2025 יצאה 12,542 בחציון מול צפיות 111,098, יחס של 1:9,
    בעוד שבייצוא של 2026 היחס הוא 1:1.3. או ש-2025 באמת היה שונה, או
    שהמדד נשחק עם הגיל. השוואה על אותו post id עונה על זה.
    """
    print("\n" + "-" * 70)
    print("1ב. בקרה — פייסבוק 2026: API מול הייצוא, אותו post id")
    if not os.path.exists(FB_2026):
        print("   ❌ ייצוא 2026 לא נמצא")
        return
    rows = []
    with open(FB_2026, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if r.get("Views") and r.get("Reach"):
                rows.append(r)
    if not rows:
        print("   ❌ אין שורות עם מדדים")
        return
    random.seed(11)
    pick = random.sample(rows, min(SAMPLE, len(rows)))
    print("   נדגמו %d פוסטים מ-2026" % len(pick))
    print("   %-26s %12s %12s   %12s %12s" % (
        "post id", "views ייצוא", "views API", "reach ייצוא", "reach API"))
    ratios_v, ratios_r = [], []
    for r in pick:
        pid = r.get("Post ID", "")
        ev = int(float(r["Views"])) if r["Views"] else 0
        er = int(float(r["Reach"])) if r["Reach"] else 0
        av, _ = insight(pid, "post_media_view")
        ar, _ = insight(pid, "post_total_media_view_unique")
        if ev and isinstance(av, (int, float)):
            ratios_v.append(av / ev)
        if er and isinstance(ar, (int, float)):
            ratios_r.append(ar / er)
        print("   %-26s %12s %12s   %12s %12s" % (
            pid[-24:], format(ev, ","),
            format(int(av), ",") if isinstance(av, (int, float)) else "-",
            format(er, ","),
            format(int(ar), ",") if isinstance(ar, (int, float)) else "-"))
    print()
    for name, rs in (("views", ratios_v), ("reach", ratios_r)):
        if not rs:
            print("   %s: אין מה להשוות" % name)
            continue
        med = sorted(rs)[len(rs) // 2]
        verdict = "✅ תואם" if 0.9 <= med <= 1.1 else "❌ לא תואם"
        print("   %s: יחס API/ייצוא חציוני %.2f  %s" % (name, med, verdict))


def ig_account():
    res = get(PAGE, fields="instagram_business_account")
    return (res.get("instagram_business_account") or {}).get("id")


def probe_ig_recovery():
    """השאלה היקרה: האם ה-API מחזיר מה שהייצוא איבד, על אותם פריטים."""
    print("\n" + "-" * 70)
    print("3. אינסטגרם — שחזור ינואר–מרץ 2024 (API מול הייצוא, אותו media id)")
    if not os.path.exists(IG_2024):
        print("   ❌ קובץ הייצוא של 2024 לא נמצא")
        return
    rows = []
    with open(IG_2024, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if r.get("Account username") != "kan_news":
                continue
            pub = r.get("Publish time", "")
            # הפורמט בייצוא הוא MM/DD/YYYY
            if pub[:2] in ("01", "02", "03") and pub[6:10] == "2024":
                rows.append(r)
    if not rows:
        print("   ❌ לא נמצאו פריטים מינואר–מרץ 2024")
        return
    random.seed(7)
    pick = random.sample(rows, min(SAMPLE, len(rows)))
    print("   נדגמו %d פריטים מתוך %d" % (len(pick), len(rows)))

    hits = 0
    print("   %-20s %10s %10s   %10s %10s" % (
        "media id", "reach ייצוא", "reach API", "views ייצוא", "views API"))
    for r in pick:
        mid = r.get("Post ID", "")
        exp_reach = r.get("Reach", "") or "-"
        exp_views = r.get("Views", "") or "-"
        api_reach, e1 = insight(mid, "reach")
        api_views, e2 = insight(mid, "views")
        if api_reach is not None and api_reach > 1000:
            hits += 1
        print("   %-20s %10s %10s   %10s %10s" % (
            mid[-18:], exp_reach,
            format(api_reach, ",") if api_reach is not None else (e1 or "-")[:10],
            exp_views,
            format(api_views, ",") if api_views is not None else (e2 or "-")[:10]))
    print()
    if hits >= len(pick) * 0.6:
        print("   ✅ שחזור אפשרי — ה-API מחזיר חשיפה שפויה למה שהייצוא איבד.")
        print("      שווה משיכה מלאה של ינואר–אוגוסט 2024.")
    else:
        print("   ❌ אין שחזור — ה-API לא נותן יותר מהייצוא. הגבול בספטמבר 2024 נשאר.")


def probe_page_series():
    print("\n" + "-" * 70)
    print("4. תובנות העמוד — כמה אחורה since/until מחזיר")
    for metric in ["page_impressions", "page_post_engagements",
                   "page_total_media_view_unique"]:
        print("\n   === %s" % metric)
        for since, until, label in [("2024-02-01", "2024-02-15", "2024"),
                                    ("2025-02-01", "2025-02-15", "2025"),
                                    ("2026-07-01", "2026-07-15", "2026")]:
            res = get("%s/insights" % PAGE, metric=metric, period="day",
                      since=since, until=until)
            e = err(res)
            if e:
                print("      %s  ❌ %s" % (label, e))
                continue
            vals = []
            for b in res.get("data", []) or []:
                vals = b.get("values", []) or []
            if not vals:
                print("      %s  (ריק)" % label)
            else:
                nums = [v.get("value") for v in vals if isinstance(v.get("value"), int)]
                print("      %s  %d ימים | %s → %s | חציון %s" % (
                    label, len(vals), str(vals[0].get("end_time"))[:10],
                    str(vals[-1].get("end_time"))[:10],
                    format(sorted(nums)[len(nums) // 2], ",") if nums else "-"))


def main():
    if not TOKEN:
        raise SystemExit("❌ missing FACEBOOK_TOKEN")
    print("=" * 70)
    print("🔎 מה ה-Graph API עוד מחזיר — מדגם של %d" % SAMPLE)
    print("=" * 70)

    me = get(PAGE, fields="name,followers_count")
    if err(me):
        raise SystemExit("❌ %s" % err(me))
    print("עמוד: %s — %s עוקבים" % (
        me.get("name"), format(me.get("followers_count", 0), ",")))

    probe_fb_posts("1. פייסבוק 2025 ברמת פוסט", "2025-05-01", "2025-05-03")
    probe_fb_control()
    probe_fb_posts("2. פייסבוק 2024 ברמת פוסט", "2024-03-01", "2024-03-03")

    ig = ig_account()
    if ig:
        probe_ig_recovery()
    else:
        print("\n⚠️ אין instagram_business_account על העמוד")

    probe_page_series()

    print("\n" + "=" * 70)
    print("סיכום: כל ✅ למעלה הוא נתון שאפשר למשוך בלי ייצוא ידני.")


if __name__ == "__main__":
    main()
