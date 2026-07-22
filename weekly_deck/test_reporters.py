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
    # Kan appends the programme hashtag straight after the credit; it used to be
    # swallowed into the captured name, which then failed the name test
    ("דיווח מהשטח. כתב: רועי קייס #כאןבשלוש", mapped('רועי קייס', 'רועי קייס'),
     "a programme hashtag right after a credit does not swallow the name"),
    # the possessive form carries no colon — how YouTube descriptions credit
    ("הכתבה ששודרה הערב. כתבתו של מואב ורדי", mapped('מואב ורדי', 'מואב ורדי'),
     "possessive credit: כתבתו של X"),
    ("מתוך תחקירה של גילי כהן ששודר אמש", mapped('גילי כהן', 'גילי כהן'),
     "possessive credit: תחקירה של X"),
    ("כתבתו של הכתב שלנו מהשטח", "",
     "a generic reference is not a person, even in the possessive form"),
    # the trailing-name rules fire at the END, so an appended tag used to hide them
    ("ועו״ד מטעם ארגון חננו מסייע להן בייעוץ משפטי | דנה שרון #בחציהיום", "דנה שרון",
     "a trailing name is still found behind an appended programme hashtag"),
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

    # --- Hebrew names carried by the map ----------------------------------
    # A byline with no parentheses, no handle and no "כתב:" only resolves because
    # the map says the string is a person. This is what lets a name learned by
    # hand one week be picked up automatically the next.
    ("הכתבה המלאה על ההסכם. מזל מועלם", mapped('מזל מועלם', ''), "Hebrew name from the map"),
    ("ראיון הבוקר עם השר. אריה גולן " + CAM + ": פלאש 90", mapped('אריה גולן', ''),
     "mapped Hebrew name before a photo credit"),

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
     "הטרנד החדש שכובש את חטיבות הביניים: בני נוער משקיעים עשרות…",
     "long headline capped, cut on a clause boundary"),
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


# trim_to_clause(s, cap) — nothing is dropped any more; the headline is only
# capped, and the cut lands on a clause boundary rather than mid-phrase.
CLAUSES = [
    ("הטרנד החדש שכובש את חטיבות הביניים: בני נוער משקיעים עשרות אלפי דולרים בבורסה",
     "הטרנד החדש שכובש את חטיבות הביניים: בני נוער משקיעים עשרות…",
     "the news after the colon is kept, not thrown away for the teaser"),
    ("רצח הצעיר בירושלים: משפחתו חושפת - \"הכתובת הייתה על הקיר\"",
     "רצח הצעיר בירושלים: משפחתו חושפת - \"הכתובת הייתה על הקיר\"",
     "already inside the cap — left alone"),
    ("יוליה בת ה-33 יצאה לצלילה חופשית באילת, כשלפתע הרגישה מכה חזקה בצוואר ומשיכה",
     "יוליה בת ה-33 יצאה לצלילה חופשית באילת…",
     "cut placed on a comma rather than mid-phrase"),
    ("\"הרגשה של נטישה\": כרם ונפתלי בני ה-18 רק רצו לחגוג את סיום התיכון בחופשה "
     "במונטנגרו יחד עם חבריהם, אבל הותקפו באלימות קשה",
     "\"הרגשה של נטישה\": כרם ונפתלי בני ה-18 רק רצו לחגוג את סיום…",
     "the opening quote stays — it is part of the story, not noise"),
    ("אחרי שצוות CNN טען שהותקף על ידי פורעים יהודים - תיעוד מתוך האירוע התפרסם",
     "אחרי שצוות CNN טען שהותקף על ידי פורעים יהודים…",
     "a dash boundary inside the cap is a clean place to stop"),
]


def _split_cases():
    """An editor filling credits by hand states the programme where it is natural
    to state it — inside the name. It belongs in the programme column."""
    return [
        (g.split_override('אריה גולן (מתוך הבוקר הזה)'), ('אריה גולן', 'הבוקר הזה'),
         "programme pulled out of the credit"),
        (g.split_override('יפעת גליק ותמר אלמוג (מתוך התוכנית גליק ותמר)'),
         ('יפעת גליק ותמר אלמוג', 'גליק ותמר'),
         "two reporters, and 'התוכנית' is not part of the programme name"),
        (g.split_override('רן בנימיני ומזל מועלם'), ('רן בנימיני ומזל מועלם', ''),
         "two reporters, no programme"),
        (g.split_override(''), ('', ''), "an empty veto stays empty"),
        (g.split_override('יואב לימור (כתב צבאי)'), ('יואב לימור (כתב צבאי)', ''),
         "parentheses that are not a programme are left alone"),
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
    ran = 0

    print("reporter extraction")
    for caption, expected, label in CASES:
        got = g.resolve_reporter(caption, RMAP)
        ran += 1
        ok = got == expected
        failures += 0 if ok else 1
        print(("  PASS  " if ok else "  FAIL  ") + label
              + ("" if ok else f"\n          got {got!r}, expected {expected!r}"))

    print("\nheadline extraction")
    for caption, expected, label in HEADLINES:
        got = g.headline_of(caption)
        ran += 1
        ok = got == expected
        failures += 0 if ok else 1
        print(("  PASS  " if ok else "  FAIL  ") + label
              + ("" if ok else f"\n          got {got!r}, expected {expected!r}"))

    print("\nheadline extraction with a known credit")
    for caption, reporter, expected, label in HEADLINES_CREDITED:
        got = g.headline_of(caption, reporter)
        ran += 1
        ok = got == expected
        failures += 0 if ok else 1
        print(("  PASS  " if ok else "  FAIL  ") + label
              + ("" if ok else f"\n          got {got!r}, expected {expected!r}"))

    print("\nclause trimming")
    for src, expected, label in CLAUSES:
        got = g.trim_to_clause(src, 62)
        ran += 1
        ok = got == expected
        failures += 0 if ok else 1
        print(("  PASS  " if ok else "  FAIL  ") + label
              + ("" if ok else f"\n          got {got!r}, expected {expected!r}"))

    print("\noverride value parsing")
    for got, expected, label in _split_cases():
        ran += 1
        ok = got == expected
        failures += 0 if ok else 1
        print(("  PASS  " if ok else "  FAIL  ") + label
              + ("" if ok else f"\n          got {got!r}, expected {expected!r}"))

    print("\nper-item overrides")
    for got, expected, label in _override_cases():
        ran += 1
        ok = got == expected
        failures += 0 if ok else 1
        print(("  PASS  " if ok else "  FAIL  ") + label
              + ("" if ok else f"\n          got {got!r}, expected {expected!r}"))

    # counted from what actually ran, so a section that is never iterated can no
    # longer inflate the total — that is exactly how five assertions went
    # unnoticed while the suite reported all green
    total = ran
    print(f"\n{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
