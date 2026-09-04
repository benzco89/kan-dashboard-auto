"""Which Kan outlet an item came from, when the item itself does not say so in
a field — shared by the dashboard and the weekly deck so the two can never
disagree about what counts as radio.

**Reshet Bet.** Radio items are posted to the Kan News *Facebook page*, not to a
page of their own, and the only marker is a signature near the end of the
caption. Two shapes appear, and both have to be matched:

    כאן חדשות ברשת ב 🎙️ אסתי פרז בן עמי        the newsroom sign-off
    ... סיפר על התקיפה בתוכנית "קלמן וליברמן" בכאן רשת ב'
    "..."  🎙 כאן רשת ב'                          the station alone

Anchoring on "כאן חדשות" alone — which is what the first version did — found
134 of them and missed 30, every one of which was genuinely radio: the ones
signed `🎙 כאן רשת ב'`, `לכאן רשת ב'`, `סיפר עליו לרשת ב'`.

The apostrophe is the second anchor and it is what makes a bare match safe. A
plain "רשת ב" hits ordinary Hebrew — `מעלה לרשת בשפה הערבית`,
`הפופולאריות של החיות ברשת בשנים האחרונות`, and a chain called
`רשת בית הפנקייק`. With a geresh it does not.

Measured over the full Facebook sheet (4,416 rows, 2026-07-26): 164 matches,
a strict superset of the 134 the old pattern found, and of the 7 rows that say
"רשת ב" and are left out, all 7 are genuinely not radio. No known miss, no
known false positive.
"""

import re

# "כאן [חדשות] [ב]רשת ב"  |  "[ב]רשת ב" carrying an apostrophe
RESHET_BET_RE = re.compile(r"כאן\s+(?:חדשות\s+)?ב?רשת ב|ב?רשת ב['׳’]")

RESHET_BET_NAME = "רשת ב׳"


def is_reshet_bet(text):
    """True when a caption carries the Reshet Bet signature."""
    return bool(RESHET_BET_RE.search(str(text or "")))


# ---------------------------------------------------------------------------
# ארכיון הווידאו, ציר 1: מי עומד מאחורי הפריט.
#
# הכיתוב שמטא מחזירה נכתב בעורך RTL ונושא סימני bidi בתוך הטקסט. **הסדר שבו
# מנקים אותם הוא באג**, ולכל סמן כלל משלו - ראו test_content_tags.py למספרים:
#   @handle - מחלצים מהגולמי (ניקוי מוקדם מייצר 52 ידיות מושחתות מתוך 212)
#   בייליין - הרגקס חייב לסבול bidi אחרי הסוגר (בלעדיו 19 במקום 34)
#   האשטאג  - אדיש
# ---------------------------------------------------------------------------

BIDI_MARKS = ("\u200b\u200c\u200d\u200e\u200f"
              "\u202a\u202b\u202c\u202d\u202e"
              "\u2066\u2067\u2068\u2069\ufeff")
_BIDI_RE = re.compile(f"[{BIDI_MARKS}]")

# הידית נעצרת בסימן bidi - זה בדיוק מה שמונע את הזנב הכפול
_HANDLE_RE = re.compile(r"@([A-Za-z0-9_.]+)")
_HASHTAG_RE = re.compile(r"#([\w֐-׿_]+)")
# בייליין = סוגריים בסוף הכיתוב, אחרי דילוג על bidi ורווחים
_BYLINE_RE = re.compile(r"\(([^()]{2,60})\)[" + BIDI_MARKS + r"\s]*$")
# מה שנראה כמו בייליין אבל אינו: קרדיטי צילום והערות הפקה
_NOT_A_BYLINE = re.compile(r"אילוסטרציה|בתמונה|צילום|דוברות|כל התמונות|"
                           r"צולם|ארכיון|רויטרס|AP|AFP")


def strip_bidi(text):
    """מסיר סימני bidi. לא להריץ לפני חילוץ ידיות - ראו הערת הבלוק."""
    return _BIDI_RE.sub("", str(text or ""))


def extract_handles(caption):
    """ידיות @ מהכיתוב **הגולמי**, לפי סדר הופעה, בלי כפילויות."""
    seen, out = set(), []
    for h in _HANDLE_RE.findall(str(caption or "")):
        if h.lower() not in seen:
            seen.add(h.lower())
            out.append(h)
    return out


def extract_hashtags(caption):
    """האשטאגים לפי סדר הופעה, בלי כפילויות."""
    seen, out = set(), []
    for h in _HASHTAG_RE.findall(strip_bidi(caption)):
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out


def extract_byline(caption):
    """שמות מתוך סוגריים בסוף הכיתוב. ריק כשאלה קרדיטי צילום ולא כתבים."""
    m = _BYLINE_RE.search(str(caption or "").rstrip())
    if not m:
        return []
    inner = strip_bidi(m.group(1)).strip()
    if _NOT_A_BYLINE.search(inner):
        return []
    names = [n.strip() for n in re.split(r",| ו(?=[א-ת])", inner)
             if n.strip()]
    return [n for n in names if len(n) >= 3]
