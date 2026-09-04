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

print("\nציר 1 - אדם ותוכנית\n")

check("האשטאג תוכנית קובע תוכנית",
      ct.tag_item("קטע מהאולפן #גליקותמר", "instagram")["program"], "גליקותמר")
check("שני האיותים של בשכונה שלנו הם תוכנית אחת",
      [ct.tag_item(f"טקסט #{h}", "tiktok")["program"]
       for h in ("בשכונה_שלנו", "בשכונהשלנו")],
      ["בשכונה שלנו", "בשכונה שלנו"])
check("האשטאג נושאי אינו תוכנית, אבל הבייליין כן",
      ct.tag_item("מתכון #בשר (יפעת גליק)", "instagram")["program"], "גליקותמר")
check("האשטאג נושאי לבדו משאיר תוכנית ריקה",
      ct.tag_item("מתכון #בשר", "instagram")["program"], "")

check("ידית אינסטגרם מזוהה לאדם",
      ct.tag_item("\u202a@itayblumental\u202c", "instagram")["person"],
      "איתי בלומנטל")
check("הידית המקבילה בטיקטוק - אותו אדם",
      ct.tag_item("@itayblumental1", "tiktok")["person"], "איתי בלומנטל")
check("כינוי באינסטגרם מול שם מלא בטיקטוק",
      [ct.tag_item("@itsik_z", "instagram")["person"],
       ct.tag_item("@itsikzuarets", "tiktok")["person"]],
      ["איציק צוארץ", "איציק צוארץ"])
check("סדר שמות הפוך בין הפלטפורמות",
      [ct.tag_item("@davidovitchsharon", "instagram")["person"],
       ct.tag_item("@sharondavidovitch", "tiktok")["person"]],
      ["שרון דוידוביץ", "שרון דוידוביץ"])

check("הידית המושחתת לא מגיעה לטבלה מלכתחילה",
      ct.tag_item("\u202a@ifatglick\u202a@almogtamar\u202car\u2069",
                  "instagram")["people"],
      ["יפעת גליק", "תמר אלמוג"])
check("צמד המגישות גוזר את התוכנית",
      ct.tag_item("\u202a@ifatglick\u202a@almogtamar\u202car\u2069",
                  "instagram")["program"], "גליקותמר")
check("מקור התוכנית מדווח",
      ct.tag_item("@ifatglick", "instagram")["program_source"], "mention")

check("בייליין עם שני איותים של אותו שם",
      [ct.tag_item("טקסט (אילה חסון)", "tiktok")["person"],
       ct.tag_item("טקסט (איילה חסון)", "tiktok")["person"]],
      ["איילה חסון", "איילה חסון"])
check("כתב שאינו בטבלת התוכניות מקבל שם וריק בתוכנית",
      ct.tag_item("טקסט (ישראל רוזנר)", "tiktok"),
      {"person": "ישראל רוזנר", "people": ["ישראל רוזנר"],
       "program": "", "program_source": ""})
check("כיתוב בלי אף סמן",
      ct.tag_item("שר הביטחון הגיע לגבול הצפון", "instagram"),
      {"person": "", "people": [], "program": "", "program_source": ""})
check("סדר הקדימות: האשטאג גובר על בייליין",
      ct.tag_item("קטע #כאןבשש (ישראל רוזנר)", "tiktok")["program_source"],
      "hashtag")
check("מקור התוכנית מדווח כבייליין כשזה המקור שקבע",
      ct.tag_item("טקסט (יפעת גליק)", "instagram")["program_source"],
      "byline")

print(f"\n{PASS} passed, {FAIL} failed\n")
sys.exit(1 if FAIL else 0)
