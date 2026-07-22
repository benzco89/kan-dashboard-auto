"""
Reporter-extraction tests for the weekly deck.

Plain asserts, no test framework — run it directly:

    python weekly_deck/test_reporters.py

Every case here came from a real Kan caption that broke something. Map-dependent
cases assert against reporters_map.json's own value, so growing the map never
breaks the suite — what is being tested is that the PARSER hands the right key
to the map, not what the map happens to contain today.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

from weekly_deck import generate_deck as g   # noqa: E402

RMAP = g.load_reporters_map()

# bidi controls that RTL captions wrap handles in
LRE, PDF = '‪', '‬'
CAM = '\U0001F4F8'


def mapped(handle, fallback):
    """What the map should turn `handle` into, or the raw handle when unmapped."""
    return RMAP.get(handle.lower(), fallback)


CASES = [
    # (caption, expected reporter, label)

    # --- explicit credits -------------------------------------------------
    ("דיווח מהשטח על הפינוי. כתב: רועי קייס", mapped('רועי קייס', 'רועי קייס'), "כתב: credit"),
    ("תיעוד: רגע הפגיעה בעוטף (יואב לימור)", "יואב לימור", "trailing parens name"),
    ("הרגע שבו נעצר החשוד (צילום: דוד לוי) (איתי בלומנטל)", "איתי בלומנטל",
     "reporter after a photo credit — do not truncate at צילום:"),
    ("תיעוד נדיר מהזירה (צילום: מוטי מילרוד)", "", "photographer only"),
    ("סיכום השבוע (עריכה: נועה כהן)", "", "editor only"),

    # --- handles ----------------------------------------------------------
    ("הכתבה של @MoavVardi מהשטח", mapped('moavvardi', '@MoavVardi'), "plain handle"),
    # the exact real caption that produced "@itamar" before the parser fix
    ("בעקבות הפרסום: ״אני בודק את האפשרויות עם המחלקה המשפטית שלנו״ "
     + LRE + "@itamar.margalit" + PDF + " " + CAM + ": הניו יורק טיימס",
     mapped('itamar.margalit', '@itamar.margalit'),
     "dotted handle wrapped in bidi controls, followed by a camera credit"),
    ("הכתבה המלאה מאת @dana.levi.", mapped('dana.levi', '@dana.levi'),
     "dotted handle at a sentence end — keep the dot, drop the full stop"),
    ("עקבו אחרי @someone_new_", "@someone_new", "trailing underscore stripped"),

    # --- the account's own handle is never a reporter ----------------------
    ("סרטון מלא בערוץ שלנו @kan_news", "", "brand handle"),
    ("צפו בערוץ " + LRE + "@kann_news" + PDF, "", "brand handle, bidi-wrapped"),
    ("הכתבה של @MoavVardi בערוץ @kan_news", mapped('moavvardi', '@MoavVardi'),
     "brand skipped, real handle still wins"),

    # --- role suffix ------------------------------------------------------
    ("סיפרה. Ketty Dor איציק זוארץ, כתב כאן11 בדרום " + CAM + ": מרכז רפואי יוספטל",
     mapped('איציק זוארץ', 'איציק זוארץ'),
     "role suffix outranks the preceding bare Latin name"),
    ("הדיווח המלא. Ketty Dor, כתבת כאן חדשות", mapped('ketty dor', 'Ketty Dor'),
     "Latin name + role suffix"),
    ("ניתוח השבוע מאת רן כהן, פרשן צבאי", "רן כהן", "leading preposition trimmed"),

    # --- camera/clapper opens a media credit that runs to the end ----------
    ("תיעוד מהזירה בשעות הבוקר " + CAM + ": Ebrahim Noroozi", "",
     "agency photographer after the camera marker"),
    ("תיעוד מהזירה " + CAM + ": אבי דישי, פלאש90", "", "photo credit, Hebrew"),
    ("הכתבה המלאה | https://bit.ly/x Dana Levi " + CAM + ": אבי דישי", "Dana Levi",
     "name BEFORE the marker still resolves"),

    # --- bare trailing names (Facebook shape) ------------------------------
    ("קטטה בדירת Airbnb | https://bit.ly/4vYM1NT Dana Levi Yossi Cohen", "Dana Levi",
     "two Latin names — first (the reporter) wins"),
    ("הכתבה המלאה https://bit.ly/3kQz9Lm Noa Berman", "Noa Berman", "one Latin name"),
    ("״ Itay Blumental " + CAM + ": אבי דישי", mapped('itay blumental', 'Itay Blumental'),
     "Latin byline derived from the @ItayBlumental map line"),
    ("ועו״ד מטעם ארגון חננו מסייע להן בייעוץ משפטי | דנה שרון", "דנה שרון",
     "bare Hebrew name set off by a separator"),

    # --- and the ones that must stay empty rather than invent a credit -----
    ("ועו״ד מטעם ארגון חננו מסייע להן בייעוץ משפטי", "",
     "bare Hebrew tail with no separator — must NOT be read as a name"),
    ("רגע מרגש בכנסת — רילס. הכתבה המלאה לצד ניתוח של מה צפוי לקרות הלאה", "",
     "a dash elsewhere in the caption must not unlock the Hebrew rule"),
    ("הכתבה המלאה עם כל הפרטים לצד ניתוח של מה צפוי לקרות הלאה", "", "plain prose"),
    ("תיעוד מהזירה | https://bit.ly/x צילום מוטי מילרוד", "",
     "role word anywhere in the trailing run disqualifies it"),
]

HEADLINES = [
    ("הטרנד החדש שכובש את חטיבות הביניים: בני נוער משקיעים עשרות אלפי דולרים בבורסה, "
     "מוחקים את החסכונות. כתבה מיוחדת (יותם ווקס)",
     "הטרנד החדש שכובש את חטיבות הביניים: בני נוער משקיעים עשרות אלפי דולרים בבורסה,…",
     "colon kept, cut at 80 on a word boundary"),
    ("פרסום ראשון: המסמך שחושף הכל \U0001F447 קישור בתגובות", "פרסום ראשון: המסמך שחושף הכל",
     "cut at the pointer emoji"),
    ("שר הביטחון הגיש בקשה למחיקת הרישום\nפרטים נוספים בהמשך",
     "שר הביטחון הגיש בקשה למחיקת הרישום", "cut at the line break"),
    ("חמישה מילימטרים בלבד מאסון. הדייג באילת חשב שתפס דג גדול",
     "חמישה מילימטרים בלבד מאסון", "cut at the sentence end"),
    (LRE + "פרסום ראשון: המסמך" + PDF + ". ועוד טקסט", "פרסום ראשון: המסמך",
     "bidi controls stripped"),
]

# headline_of(caption, reporter) — once the credit is known it stops being part
# of the headline. RTL captions routinely glue the handle onto the last word.
HEADLINES_CREDITED = [
    ("ג'פניקה הפכה לזירת קרב: הסכסוך של משפחות הפשע הוביל לאש@hadasgrinberg",
     "הדס גרינברג", "ג'פניקה הפכה לזירת קרב: הסכסוך של משפחות הפשע הוביל לאש",
     "handle glued to the last word is dropped"),
    ("שר הביטחון הגיש בקשה למחיקת הרישום @MoavVardi", "מואב ורדי",
     "שר הביטחון הגיש בקשה למחיקת הרישום", "spaced handle is dropped"),
    ("הדייג באילת חשב שתפס דג גדול (יותם ווקס)", "יותם ווקס",
     "הדייג באילת חשב שתפס דג גדול", "parenthesised credit is dropped"),
    ("תיעוד מהזירה — איתי בלומנטל", "איתי בלומנטל", "תיעוד מהזירה",
     "bare trailing name and its separator are dropped"),
    ("המשקיעים החדשים בבורסה @kann_news", "", "המשקיעים החדשים בבורסה @kann_news",
     "with NO reporter the handle stays — it is the hint that the map is missing a line"),
]


def _override_cases():
    """apply_reporter_overrides is what makes a hand-filled credit survive both a
    re-render and a re-extract, so it is worth pinning down."""
    content = dict(platforms=[dict(key='youtube', top=[
        dict(id='vid1', reporter=''),            # no credit -> filled
        dict(id='vid2', reporter='מישהו אחר'),   # wrong credit -> replaced
        dict(id='vid3', reporter='גילי כהן'),    # not in the file -> untouched
        dict(id='vid4', reporter='טעות'),        # empty value -> deliberate veto
    ])])
    ov = {'youtube:vid1': 'איתי בלומנטל', 'youtube:vid2': 'מואב ורדי',
          'youtube:vid4': ''}
    filled, vetoed = g.apply_reporter_overrides(content, ov)
    top = content['platforms'][0]['top']
    return [
        (top[0]['reporter'], 'איתי בלומנטל', "an uncredited item gets its name"),
        (top[1]['reporter'], 'מואב ורדי', "an override beats an auto-resolved name"),
        (top[2]['reporter'], 'גילי כהן', "an item with no override is untouched"),
        (top[3]['reporter'], '', 'an empty value vetoes a wrong auto-credit'),
        (top[3]['_override'], True, "a vetoed item is marked, so it leaves the TODO list"),
        ((filled, vetoed), (2, 1), "counts reported back to the caller"),
    ]


def main():
    failures = 0

    print("reporter extraction")
    for caption, expected, label in CASES:
        got = g.resolve_reporter(caption, RMAP)
        ok = got == expected
        failures += 0 if ok else 1
        print(("  PASS  " if ok else "  FAIL  ") + label
              + ("" if ok else f"\n          got {got!r}, expected {expected!r}"))

    print("\nheadline extraction")
    for caption, expected, label in HEADLINES:
        got = g.headline_of(caption)
        ok = got == expected
        failures += 0 if ok else 1
        print(("  PASS  " if ok else "  FAIL  ") + label
              + ("" if ok else f"\n          got {got!r}, expected {expected!r}"))

    print("\nheadline extraction with a known credit")
    for caption, reporter, expected, label in HEADLINES_CREDITED:
        got = g.headline_of(caption, reporter)
        ok = got == expected
        failures += 0 if ok else 1
        print(("  PASS  " if ok else "  FAIL  ") + label
              + ("" if ok else f"\n          got {got!r}, expected {expected!r}"))

    print("\nper-item overrides")
    for got, expected, label in _override_cases():
        ok = got == expected
        failures += 0 if ok else 1
        print(("  PASS  " if ok else "  FAIL  ") + label
              + ("" if ok else f"\n          got {got!r}, expected {expected!r}"))

    total = len(CASES) + len(HEADLINES) + len(HEADLINES_CREDITED) + len(_override_cases())
    print(f"\n{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
