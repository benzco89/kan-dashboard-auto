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


# האשטאג = "קטע תוכנית" בדיוק ~99%, אבל שם את התוכנית רק ב-84% מהמקרים
# ורואה רק 9% מהפריטים. הוא מסנן טוב וספירה גרועה - לכן הוא רק הסמן הראשון
# מתוך שלושה, ולא המקור היחיד.
PROGRAM_BY_HASHTAG = {
    "גליקותמר": "גליקותמר",
    "גליקולתמר": "גליקותמר",
    "כאןבשש": "כאן בשש",
    "כאןבשלוש": "כאן בשלוש",
    "חדשותהלילה": "חדשות הלילה",
    "בשכונה_שלנו": "בשכונה שלנו",
    "בשכונהשלנו": "בשכונה שלנו",
    "שובר_חומות": "שובר חומות",
    "סיפור_עולמי": "סיפור עולמי",
    "סיפור_דרך": "סיפור דרך",
    "תקופת_המנדט": "תקופת המנדט",
    "הפרלמנטשלאוריה": "הפרלמנט של אוריה",
    "בנימיניומועלם": "בנימיני ומועלם",
    "בנימיניוגואטה": "בנימיני וגואטה",
    "קלמןליברמן": "קלמן וליברמן",
    "רקשתדעו": "רק שתדעו",
    "הרגעשלהשבוע": "הרגע של השבוע",
    "האיש_בשטח": "האיש בשטח",
    "חדשות_השבת": "חדשות השבת",
    "סלסולחוזר": "סלסול חוזר",
}

# ידית -> שם. נגזר מכל הידיות בשתי הפלטפורמות: נרמול [._] והסרת ספרות סופיות
# זיווג 28 אנשים אוטומטית; המסומנים "ידני" הם מה שהנרמול לא יכול לתפוס -
# סדר שמות הפוך, כינוי, או שם משפחה שני.
HANDLE_TO_PERSON = {
    "itayblumental": "איתי בלומנטל", "itayblumental1": "איתי בלומנטל",
    "itay.blumental": "איתי בלומנטל",
    "haimgoldich": "חיים גולדיטש",
    "ifatglick": "יפעת גליק", "ifatglick1": "יפעת גליק",
    "almogtamar": "תמר אלמוג",
    "hadasgrinberg": "הדס גרינברג", "hadas.grinberg": "הדס גרינברג",
    "ramelibrandts": "רם אלי ברנדס",
    "ishay_bar_yosef": "ישי בר-יוסף",
    "daniel.grovais": "דניאל גרובייס",
    "yoavborowitz": "יואב בורוביץ",
    "maya_rachlin": "מאיה רחלין", "mayarachlin": "מאיה רחלין",
    "anna.pines": "אנה פינס", "annapines_": "אנה פינס",
    "roysharon11": "רוי שרון", "roy.sharon.71": "רוי שרון",
    "roikais": "רועי קייס", "roikais1987": "רועי קייס",
    "amitharari": "עמית הררי", "amitharari8": "עמית הררי",
    "dikla_aharon": "דקלה אהרון", "dikla.aharon": "דקלה אהרון",
    "liran_kogahinoff": "לירן קוגהינוף",
    "roi_yanovsky": "רועי ינובסקי", "roiyanovsky": "רועי ינובסקי",
    "yoav.zehavi": "יואב זהבי", "yoavzehavi10": "יואב זהבי",
    "eliorlevy": "אליאור לוי", "elior_levy": "אליאור לוי",
    "ittaishick": "איתי שיקמן", "ittaishickman2": "איתי שיקמן",  # ידני
    "ofirhalperin": "אופיר הלפרין", "ofir.halperin": "אופיר הלפרין",
    "anastasia.stu": "אנסטסיה סטו", "anastasia.stu7": "אנסטסיה סטו",
    "kereneubach": "קרן נויבך",
    "lielkyzer": "ליאל קייזר", "lielkyzer1": "ליאל קייזר",
    "kerenuzan1": "קרן אוזן",
    "riadalee": "ריאד עלי", "riad.alee": "ריאד עלי",
    "talfraz": "טל פרז", "talberman": "טל ברמן",
    "itsik_z": "איציק צוארץ", "itsikzuarets": "איציק צוארץ",  # ידני
    "yaeli_cie": "יעל צ'כנובר", "yaelciec": "יעל צ'כנובר",  # ידני
    "davidovitchsharon": "שרון דוידוביץ",
    "sharondavidovitch": "שרון דוידוביץ",  # ידני
    "shapirayaara": "יערה שפירא", "yaara.shapira": "יערה שפירא",  # ידני
    "_dorit_mizrahi": "דורית אסרף מזרחי", "dorit_mizrahi_": "דורית אסרף מזרחי",
    "doritassarafmizra": "דורית אסרף מזרחי",  # ידני
    "carmela_menashe": "כרמלה מנשה", "carmelamenashe": "כרמלה מנשה",
    "menashecarmela": "כרמלה מנשה",  # ידני
    "maylaurencefaye.bismuth": "מיי לורנס",
    "maylaurencelola": "מיי לורנס",  # ידני
    "veredpelman": "ורד פלמן", "itamar.margalit": "איתמר מרגלית",
    "rubih": "רובי המרשלג", "rubihammerschlag": "רובי המרשלג",
    "gilicohen10": "גילי כהן", "nathanguttman": "נתן גוטמן",
    "omershahar10": "עומר שחר", "omer_shahar_10": "עומר שחר",
    "asaf_pozailov": "אסף פוזיילוב", "yechez.korn": "יחזקאל קורן",
    "shahar.glick.news": "שחר גליק", "suleimanmas": "סולימאן מסאלחה",
    "wassermanmichal": "מיכל וסרמן", "nov_reuveny": "נוב ראובני",
    "itamarvish": "איתמר וישנקו", "nofarji": "נופר ג'י",
    "yonatan_ohayon": "יונתן אוחיון", "moavvardi": "מואב ורדי",
    "shemeshmicha": "מיכה שמש", "noam_goldbergg": "נעם גולדברג",
    "shalev.segal": "שלו סגל", "shalevsegal_": "שלו סגל",
    "alon__sharvit": "אלון שרביט", "alon_sharvit": "אלון שרביט",
    "lianwildau": "ליאן וילדאו", "chen_beyar": "חן ביאר",
    "uriyaelk": "אוריה אלקיים", "alon_fruchter": "אלון פרוכטר",
    "ayala_hasson": "איילה חסון", "yuvalynbar": "יובל ינבר",
    "sharonwexler": "שרון וקסלר", "ifatamiel": "יפעת עמיאל",
    "liranaharoni_": "לירן אהרוני", "daniel.elazar": "דניאל אלעזר",
    "rom_braslavski": "רום ברסלבסקי", "yuval_agassi": "יובל אגסי",
    "orenaharoni": "אורן אהרוני", "oferhalfono": "עופר חלפון",
    "israel.rosner": "ישראל רוזנר", "kettydor": "קטי דור",
    "ettingeryair": "יאיר אטינגר", "singereran": "ערן זינגר",
    "singer.eran": "ערן זינגר",
}

# איותים חלופיים של אותו שם עברי, מהבייליינים בפועל
NAME_ALIASES = {
    "אילה חסון": "איילה חסון",
    "חיים גולדברג": "חיים גולדיטש",
    "רם ברנדס": "רם אלי ברנדס",
}

# אדם -> תוכנית. מכוון להיות חלקי: כתב שאינו כאן מקבל person ותוכנית ריקה,
# וזה מצב קריא ולא שגיאה.
#
# רוב הטבלה **נכרתה מהנתונים ולא הוקלדה** - ראו mine_programs.py. פריט שנושא
# גם האשטאג של תוכנית וגם שם הוא דוגמה מתויגת; 198 כאלה מתוך 2,564 כיתובים
# (2026-09-06) מספיקים כדי לגזור את הצמדים. הסף הוא 3 מופעים ו-70%
# דומיננטיות, והוא לא קוסמטי: **לא לכל כתב יש תוכנית אחת.** אליאור לוי יצא
# 4/4/5 בין חדשות הלילה, בשכונה שלנו ושובר חומות - שיוך שלו לאחת מהן היה
# המצאה, אז הוא אינו כאן ומקבל person בלבד.
#
# הכיסוי עלה מ-11.9% ל-18.1% מהכיתובים. השורות המסומנות ידני הן החלטה
# עורכית שהכרייה **אינה דורסת**: כרמלה מנשה יצאה "כאן בשש" x3 בנתונים
# ונשארה "רשת ב׳" בהחלטת בן (2026-09-06).
PROGRAM_BY_PERSON = {
    "יפעת גליק": "גליקותמר",         # 85/85
    "תמר אלמוג": "גליקותמר",         # 62/62
    "מאיה רחלין": "כאן בשש",         # 13/17
    "קרן אוזן": "כאן בשש",           # 11/11
    "שרון דוידוביץ": "חדשות הלילה",  # 11/11
    "רועי קייס": "בשכונה שלנו",      # 9/9
    "יונתן אוחיון": "כאן בשלוש",     # 5/6
    "אנה פינס": "גליקותמר",          # 5/5
    "שלו סגל": "הרגע של השבוע",      # 3/3
    "ליאל קייזר": "כאן בשש",         # 3/3
    "קרן נויבך": "סדר יום",          # ידני
    "כרמלה מנשה": "רשת ב׳",          # ידני - ראו הערה למעלה
}


def _canon_person(name):
    n = re.sub(r"\s+", " ", str(name or "")).strip()
    return NAME_ALIASES.get(n, n)


def tag_item(caption, platform):
    """מי ומאיזו תוכנית. שלושה סמנים לפי סדר: האשטאג, בייליין, אזכור.

    person נשאר מלא גם כשהתוכנית ריקה - טבלת התוכניות חלקית בכוונה, ושם
    כתב שווה יותר מ"לא ידוע".
    """
    caption = str(caption or "")
    byline_people = [_canon_person(n) for n in extract_byline(caption)]
    people = []
    for p in byline_people:
        if p and p not in people:
            people.append(p)
    for handle in extract_handles(caption):
        p = HANDLE_TO_PERSON.get(handle.lower())
        if p and p not in people:
            people.append(p)

    program, source = "", ""
    for tag in extract_hashtags(caption):
        if tag in PROGRAM_BY_HASHTAG:
            program, source = PROGRAM_BY_HASHTAG[tag], "hashtag"
            break
    if not program:
        for p in byline_people:
            if p in PROGRAM_BY_PERSON:
                program, source = PROGRAM_BY_PERSON[p], "byline"
                break
    if not program:
        for p in people:
            if p in PROGRAM_BY_PERSON:
                program, source = PROGRAM_BY_PERSON[p], "mention"
                break

    return {"person": people[0] if people else "", "people": people,
            "program": program, "program_source": source}
