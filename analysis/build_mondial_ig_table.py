# -*- coding: utf-8 -*-
"""
בונה את טבלת פוסטי המונדיאל באינסטגרם.

אותו לקסיקון ואותה שיטה כמו בטוויטר, אבל שתי הבעיות שונות לגמרי:

  * טוויטר: חור בכיסוי. הגיליון מתחיל 21.6 והטורניר נפתח 11.6, והשלמה
    דרך אינדקס החיפוש של X מחזירה 73% בלבד.
  * אינסטגרם: אין חור בכיסוי - הגיליון מתחיל 2025-11-21 ומכסה הכל. מה
    שחסר הוא רוחב: `instagram_collector.py:306` שומר caption[:500], ו-193
    מ-685 הפוסטים בחלון (28%) נקטעו. הכי ארוך הוא 2,188 תווים, כלומר
    1,688 תווים שלא קיימים מבחינת מי שמחפש בגיליון.

לכן הסיווג כאן רץ על הכיתוב המלא מ-`instagram_caption_probe.py`, לא על
הגיליון. המדדים (צפיות, reach) עדיין מגיעים מהגיליון - ה-probe לא מבקש
insights בכוונה.

    python analysis/build_mondial_ig_table.py --captions ig_captions/*.csv
"""

import argparse
import csv
import glob
import html
import os
import re
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mondial_lexicon as L  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mondial_instagram_2026.csv")

# נתפסו במילת CORE, אינם מונדיאל 2026.
DROP = {
    "18073478090327880": "טראמפ מבטל תקיפות באיראן; המונדיאל מוזכר רק כציון זמן",
    "18093121958013375": 'יוזמת פיפ"א למשחק נוער מול פלסטין - אליפות העולם עד גיל 20',
    "18114193633898653": "מעבר של חלאילי לאינטר; מוזכר המונדיאליטו של 2023",
}

# מונדיאל בלי המילה "מונדיאל".
KEEP = {
    "17872276995527495": "הקהילה האיראנית בלוס אנג'לס מוחה נגד נבחרת ארצה",
    "18567986611064987": "משפט חכימי בצרפת, במהלך הטורניר",
    "18101389409142255": "האוהדים היפנים מנקים את היציע אחרי המשחק",
    "18084989426542808": "דאלאס הופכת לבואנוס איירס - שירי אוהדי ארגנטינה",
    "17944040343238929": "אווה שוורץ בת 96 צופה בכל משחק של ברזיל",
    "18102106406586305": "עידו הקוף מנחש נכון את חצאי הגמר",
}

COLUMNS = ["תאריך", "שעה", "וידאו", "סוג", "כיתוב", "קישור", "צפיות", "רשומים",
           "לייקים", "תגובות", "שמירות", "שיתופים", "נקטע בגיליון", "media_id"]

_TCO = re.compile(r"\s*https?://\S*t\.co/\w+")
REPLACEMENT = "�"
# תווי בקרה דו-כיווניים שנדבקים לכיתובים של כאן (FSI/PDI/LRM/RLM ודומיהם).
# הם בלתי נראים בעורך אבל מוצגים כתיבה ריקה בכל צרכן שלא מטפל בהם, והם
# מגיעים בצרורות בתחילת הכיתוב יחד עם טאבים.
_BIDI = re.compile(r"[‎‏‪-‮⁦-⁩﻿]")


def clean(text):
    t = _BIDI.sub("", html.unescape(text or "")).replace(REPLACEMENT, "")
    return re.sub(r"\s+", " ", _TCO.sub("", t)).strip()


def _int(v):
    try:
        return int(str(v).replace(",", "") or 0)
    except (ValueError, TypeError):
        return 0


def load_sheet():
    from social_dashboard import gsheets
    return {r["media_id"]: r for r in gsheets.get_data(keys=["instagram"])["instagram"]}


def load_captions(patterns):
    out = {}
    for pat in patterns:
        for path in sorted(glob.glob(pat)):
            with open(path, encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    out[r["media_id"]] = r
            print("   + %s" % path)
    return out


def select(posts):
    picked, review = [], []
    for r in posts.values():
        mid = r["media_id"]
        core, strong, weak = L.signals(r["caption_full"])
        if mid in DROP:
            continue
        if mid in KEEP or core:
            picked.append(r)
        elif L.TOURNAMENT_START <= r["date"] <= L.AFTERMATH_END and (strong or len(weak) >= 2):
            review.append(r)
    return picked, review


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--captions", nargs="+", required=True,
                    help="CSVs מ-instagram_caption_probe.py - הכיתוב המלא")
    ap.add_argument("--show-review", action="store_true")
    args = ap.parse_args()

    posts = load_captions(args.captions)
    sheet = load_sheet()
    print("📷 %d פוסטים עם כיתוב מלא | 📄 %d שורות בגיליון" % (len(posts), len(sheet)))

    cut = [r for r in posts.values() if int(r["caption_len"]) > 500]
    print("✂️  %d מהם (%.0f%%) ארוכים מ-500 תווים, כלומר נקטעו בגיליון"
          % (len(cut), 100.0 * len(cut) / max(len(posts), 1)))

    picked, review = select(posts)
    picked.sort(key=lambda r: (r["date"], r["time"]))

    missing = (set(KEEP) | set(DROP)) - set(posts)
    if missing:
        print("⚠️  הכרעות ידניות שלא נמצא להן פוסט: %s" % ", ".join(sorted(missing)))

    # כמה מהנבחרים היו מתפספסים אילו חיפשנו בגיליון הקטום?
    hidden = [r for r in picked
              if L.signals(r["caption_full"])[0]
              and not L.signals((sheet.get(r["media_id"]) or {}).get("caption", "") or "")[0]
              and r["media_id"] not in KEEP]

    with open(OUT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        for r in picked:
            s = sheet.get(r["media_id"], {})
            w.writerow([r["date"], r["time"], "כן" if r["type"] == "Reel" else "לא",
                        {"Reel": "ריל", "Photo": "תמונה", "Carousel": "קרוסלה"}.get(r["type"], r["type"]),
                        clean(r["caption_full"]), r["permalink"],
                        _int(s.get("views")), _int(s.get("reach")),
                        _int(r.get("likes")), _int(r.get("comments")),
                        _int(s.get("saved")), _int(s.get("shares")),
                        "כן" if int(r["caption_len"]) > 500 else "לא",
                        r["media_id"]])

    reels = sum(1 for r in picked if r["type"] == "Reel")
    print("\n✅ %d פוסטי מונדיאל (%d רילס), %s → %s"
          % (len(picked), reels, picked[0]["date"], picked[-1]["date"]))
    print("   %s צפיות מצטברות" % format(
        sum(_int((sheet.get(r["media_id"]) or {}).get("views")) for r in picked), ","))
    print("   %d מהם הקיטום בגיליון היה מסתיר" % len(hidden))
    print("   נשמר: %s" % OUT)
    print("   %d מועמדים ללא הכרעה (--show-review)" % len(review))

    if args.show_review:
        for r in sorted(review, key=lambda x: (x["date"], x["time"])):
            print("   %s %s [%s] %s" % (r["date"], r["time"], r["type"], r["caption_full"][:95]))


if __name__ == "__main__":
    main()
