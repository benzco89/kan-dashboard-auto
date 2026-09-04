# -*- coding: utf-8 -*-
"""נועל את ציר 1 של ארכיון הווידאו - חילוץ הסמנים מהכיתוב.

הסדר הוא הבאג. הכיתוב שמטא מחזירה נושא סימני bidi מהעורך שבו הוא נכתב, ולכל
סמן יש כלל אחר:

  @handle  - מחלצים מהטקסט **הגולמי**. ניקוי bidi קודם מדביק את הזנב שאחרי
             U+202C להתחלה ומייצר ידיות שלא קיימות: נמדד על גיליון האינסטגרם,
             212 ידיות ייחודיות מהן 52 מושחתות (almogtamarar, ifatglickck,
             roikaisisisis) מול 165 ואפס מושחתות בחילוץ מהגולמי - באותם
             1,982 טוקנים בדיוק, כלומר לא מאבדים אזכור אחד.
  בייליין   - חייב לסבול סימני bidi **אחרי** הסוגר הסוגר. בלי זה נמצאו 19
             בייליינים באינסטגרם במקום 34 - אובדן של 44%.
  #האשטאג  - אדיש לסדר (12 ייחודיים / 39 טוקנים בשני המסלולים).

    python test_content_tags.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "social_dashboard"))
import content_tags as ct  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" +
          ("" if ok else f"\n        got  {got!r}\n        want {want!r}"))


print("\nציר 1 - חילוץ סמנים\n")

# שורות אמיתיות מ-analysis/presentation/pulled/sheet_instagram.csv
check("שתי ידיות צמודות שמופרדות בסימן bidi",
      ct.extract_handles("וד בצבע  \u202a@ifatglick\u202a@almogtamar\u202car\u2069"),
      ["ifatglick", "almogtamar"])
check("זנב חוזר אחרי U+202C לא נבלע לתוך הידית",
      ct.extract_handles("ירות\"  \u202a\u202a\u202a@gilicohen10\u202a@roikais"
                         "\u202cis\u202cis\u202cis 📸: AP"),
      ["gilicohen10", "roikais"])
check("ידית בודדת עטופה",
      ct.extract_handles(" שלושה.  \u202a@nathanguttman\u202c  📸: AP\u2069"),
      ["nathanguttman"])
check("אין ידיות",
      ct.extract_handles("כותרת בלי אף אזכור"), [])

check("בייליין עם סימן bidi אחרי הסוגר",
      ct.extract_byline("\u2066טקסט הידיעה (דב גיל-הר)\u2069"), ["דב גיל-הר"])
check("בייליין עם שני שמות",
      ct.extract_byline("טקסט (אורלי אלקלעי, הדס גרינברג)"),
      ["אורלי אלקלעי", "הדס גרינברג"])
check("בייליין עם ו' חיבור",
      ct.extract_byline("טקסט (חיים גולדברג ואבשלום ששוני)"),
      ["חיים גולדברג", "אבשלום ששוני"])
check("סוגריים באמצע אינם בייליין",
      ct.extract_byline("(לפי הדיווח) הכוחות נכנסו לעיר"), [])
check("קרדיט צילום אינו בייליין",
      ct.extract_byline("טקסט (תמונת אילוסטרציה)"), [])

check("האשטאג עברי",
      ct.extract_hashtags("קטע מהתוכנית #גליקותמר"), ["גליקותמר"])
check("האשטאג עם קו תחתון",
      ct.extract_hashtags("#בשכונה_שלנו ועוד"), ["בשכונה_שלנו"])

check("strip_bidi מנקה בלי לגעת בטקסט",
      ct.strip_bidi("\u202aשלום\u202c\u2069"), "שלום")

print(f"\n{PASS} passed, {FAIL} failed\n")
sys.exit(1 if FAIL else 0)
