# -*- coding: utf-8 -*-
"""
בונה את טבלת ציוצי המונדיאל.

מקורות:
  * הגיליון "נתוני טוויטר" (מתחיל 2026-06-21)
  * כל CSV שמגיע מ-twitter_search_probe.py, דרך --extra - שם נמצאים
    הימים שהגיליון לא מכסה (11-20.6, תחילת הטורניר)

הסיווג הוא לקסיקון + הכרעות ידניות, ובמפורש לא "מה שהמילה תפסה":
  * CORE תופס לבד. STRONG/WEAK רק מייצרים מועמדים לסקירה.
  * DROP  - נתפס במילת CORE אבל אינו מונדיאל (ראו הנימוק ליד כל שורה).
  * KEEP  - מונדיאל שלא אומר "מונדיאל" באף מילה. אלה נמצאו בסריקה הרחבה,
            והם 22% מהטבלה - חיפוש מילולי לבדו היה מפספס אותם.

    python analysis/build_mondial_table.py [--extra twitter_search/*.csv]
"""

import argparse
import csv
import glob
import os
import sys

# הקונסולה של Windows היא cp1255 כברירת מחדל ונופלת על אימוג'י ועל עברית.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mondial_lexicon as L  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mondial_tweets_2026.csv")

# ההכרעות נשמרות לפי tweet_id ולא לפי (תאריך, שעה). בגיליון יש 66 זוגות
# ציוצים שנשלחו באותה דקה, ומפתח לפי דקה בוחר מהם שרירותית: בגרסה הראשונה
# של הטבלה זה הכניס את "חוק יסוד לימוד תורה רוכך" במקום הכוננות בפריז.
DROP = {
    "2073748889031585819": "כתב אישום נגד מאמן התעמלות - 'נבחרת ישראל'",
    "2073905813110796598": "אלוף העולם בפוקר",
    "2089269944600936790": "טל ברודי בפריימריז - כדורסל",
    "2082775312996221423": "מרוץ נשיאות פיפ\"א, בלי קשר לטורניר",
    "2089733743661436938": "ישראל מסירה תמיכה מאינפנטינו - פוליטיקה של פיפ\"א",
}

# מונדיאל בלי המילה "מונדיאל".
KEEP = {
    # מהימים שהגיליון לא מכסה, דרך advanced_search
    "2064589653152342151": "'הריקוד האחרון' של מסי ורונאלדו - לקראת הטורניר",
    "2066755340062847370": "שוער כף ורדה בשידור חי בתום המשחק מול ספרד",
    "2066765843325673587": "שוער כף ורדה מגיע ל-4.5 מיליון עוקבים",
    "2067288794429063435": "מסי פורץ בבכי על הדשא אחרי שעשה היסטוריה",
    # מהגיליון
    "2072408384045793635": "נורווגיה עלתה לשמינית הגמר",
    "2073105034334605328": "שירי האוהדים - קדימון בשיא הטורניר",
    "2074530818299433301": "מסי וארגנטינה מול סלאח ומצרים",
    "2074565879803617294": "הקולות מהאצטדיון באטלנטה",
    "2074567343716368749": "במצרים כואבים את ההפסד לארגנטינה",
    "2074878359243305155": "'הגול הזה יצא מהבטן ומהלב'",
    "2075164921780711863": "אבי טולדנו לפני מרוקו-צרפת",
    "2075277056686825685": "כוננות בפריז לקראת המשחק מול מרוקו",
    "2075714818120511966": "פרעות אחרי רבע הגמר בפריז",
    "2077087296587751496": "אנגליה או ארגנטינה - מי תעלה לגמר",
    "2077097585882341563": "טל ברמן על המגרש לפני חצי הגמר",
    "2078185309074866649": "בואנוס איירס מתכסה בדגלים",
    "2078697676426604709": "'למה אנחנו בוכים מכדורגל' - ביום הגמר",
    "2078837767249838185": "'הגמר הכי פוליטי בהיסטוריה'",
    "2078864129679343737": "השקט שלפני הסערה בבואנוס איירס",
    "2078892459698839823": "הופעת המחצית של שאקירה וביבר",
    "2079199118367371500": "נבחרת ספרד חוזרת הביתה עם הגביע",
    "2079271194772029500": "ספרד אלופת העולם - החגיגות במדריד",
    "2079634966976036938": "שני מיליון ספרדים בשדרות מדריד",
    "2082033997559824843": "זידאן מונה למאמן נבחרת צרפת - המשך ישיר לטורניר",
}

COLUMNS = ["תאריך", "שעה", "וידאו", "סוג מדיה", "טקסט הציוץ", "קישור",
           "צפיות", "לייקים", "ריטוויטים", "תגובות", "שיעור מעורבות %", "מקור", "tweet_id"]


def _int(v):
    try:
        return int(str(v).replace(",", "") or 0)
    except (ValueError, TypeError):
        return 0


def load_sheet():
    from social_dashboard import gsheets
    rows = gsheets.get_data(keys=["twitter"])["twitter"]
    for r in rows:
        r["_src"] = "גיליון"
    return rows


def load_extra(patterns):
    out = []
    for pat in patterns:
        for path in sorted(glob.glob(pat)):
            with open(path, encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    r["_src"] = "advanced_search"
                    out.append(r)
            print("   + %s" % path)
    return out


def select(rows):
    """מחזיר (נבחרים, מועמדים_לסקירה). מועמד = STRONG/WEAK בלי CORE ובלי הכרעה."""
    picked, review = [], []
    for r in rows:
        key = str(r.get("tweet_id", ""))
        core, strong, weak = L.signals(r.get("text", "") or "")
        if key in DROP:
            continue
        if key in KEEP or core:
            picked.append(r)
        elif L.TOURNAMENT_START <= r.get("date", "") <= L.AFTERMATH_END and (strong or len(weak) >= 2):
            review.append((r, strong, weak))
    return picked, review


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--extra", nargs="*", default=[],
                    help="CSVs מ-twitter_search_probe.py, לימים שהגיליון לא מכסה")
    ap.add_argument("--show-review", action="store_true",
                    help="הדפס את המועמדים שהלקסיקון העלה ושאיש לא הכריע לגביהם")
    args = ap.parse_args()

    rows = load_sheet()
    print("📄 גיליון: %d ציוצים" % len(rows))
    if args.extra:
        extra = load_extra(args.extra)
        have = {str(r["tweet_id"]) for r in rows}
        new = [r for r in extra if str(r["tweet_id"]) not in have]
        print("🔎 advanced_search: %d ציוצים, מתוכם %d חדשים" % (len(extra), len(new)))
        rows = rows + new

    picked, review = select(rows)
    picked.sort(key=lambda r: (r["date"], r["time"]))

    # מזהה בהכרעה ידנית שלא נמצא בנתונים = הכרעה שלא עשתה כלום, בשקט.
    ids = {str(r.get("tweet_id", "")) for r in rows}
    missing = (set(KEEP) | set(DROP)) - ids
    if missing:
        print("⚠️  הכרעות ידניות שלא נמצא להן ציוץ: %s" % ", ".join(sorted(missing)))

    with open(OUT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        for r in picked:
            w.writerow([r["date"], r["time"], "כן" if r["type"] == "Video" else "לא",
                        r["type"], r["text"], r["permalink"], _int(r["views"]),
                        _int(r["likes"]), _int(r["retweets"]), _int(r["replies"]),
                        r.get("engagement_rate", ""), r.get("_src", ""),
                        r.get("tweet_id", "")])

    vids = sum(1 for r in picked if r["type"] == "Video")
    print("\n✅ %d ציוצי מונדיאל (%d עם וידאו), %s → %s" % (
        len(picked), vids, picked[0]["date"], picked[-1]["date"]))
    print("   %s צפיות מצטברות" % format(sum(_int(r["views"]) for r in picked), ","))
    print("   נשמר: %s" % OUT)
    print("   %d מועמדים מהלקסיקון ללא הכרעה (--show-review לראות)" % len(review))

    if args.show_review:
        for r, s, w_ in sorted(review, key=lambda x: (x[0]["date"], x[0]["time"])):
            print("   %s %s [%s] %s | %s" % (
                r["date"], r["time"], r["type"], "/".join((s or w_)[:2]), r["text"][:90]))


if __name__ == "__main__":
    main()
