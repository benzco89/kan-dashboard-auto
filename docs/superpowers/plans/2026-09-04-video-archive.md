# Video Archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pull every video Kan News publishes to Instagram and TikTok, store the file in Google Drive, classify it by person/program and by topic, and index it in a sheet a story-rail feature can read.

**Architecture:** A sibling of `hot_sniffer.py` — an intraday job fired by a VPS systemd timer through `workflow_dispatch`, discovering through the platform APIs directly (never the daily sheets), writing only its own sheet. Classification is split: axis 1 is pure text functions in the shared `social_dashboard/content_tags.py`, axis 2 is one Gemini flash call per new item. Drive access goes through a user OAuth refresh token because a service account has no storage quota of its own.

**Tech Stack:** Python 3.10, `gspread==6.2.1`, `google-api-python-client==2.198.0`, `google-genai==2.10.0`, `requests`, `pytz`. Tests follow the repo's hand-rolled `check(name, got, want)` + PASS/FAIL style (`python test_x.py`), not pytest.

**Spec:** `docs/superpowers/specs/2026-09-02-video-archive-design.md` — read it before starting. This plan implements it, with three amendments recorded in "Amendments to the spec" below.

## Global Constraints

- **Never write to a collector sheet.** `נתוני אינסטגרם`, `נתוני טיקטוק`, `נתוני פייסבוק`, `נתוני יוטיוב` carry `views_delta` columns computed against the previous run; a second writer corrupts them. This job writes exactly one sheet: `ארכיון וידאו`.
- **Never change `hot_sniffer.py`'s behaviour.** It is a live alerting job.
- **The index row is written last**, after the file is in Drive. Never the reverse.
- **The unit of failure is the item, never the run.** One item that 403s or whose Gemini call fails is logged and skipped; the run continues and the item is retried next run because it is absent from the index.
- **A media URL is never persisted.** Instagram's `media_url` and TikTok's `play_addr` expire; they are resolved and downloaded in one call path.
- **No new pip dependency.** `google-auth` (a transitive dep of `google-api-python-client`) already provides `google.oauth2.credentials.Credentials`, which is all a refresh token needs. `google-auth-oauthlib` is used only by the one-off local consent script and must not be added to `requirements.txt`.
- **Hebrew comments and docstrings**, matching every other script at the repo root.
- Spreadsheet ID: `1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c`.
- `ARCHIVE_LOOKBACK_HOURS = 48`, `ARCHIVER_VERSION = "1.0"`, index sheet name `ארכיון וידאו`.

---

## Amendments to the spec

These three came out of measuring the spec's claims against the caption data on disk
(`ig_captions/captions_2026-06-08_2026-08-11.csv`, 685 full captions;
`analysis/presentation/pulled/sheet_instagram.csv`, 2,825 rows;
`analysis/presentation/pulled/sheet_tiktok.csv`, 346 rows). Implement the amendment, not the spec, where they differ.

**A1. There is no "doubled-tail trim". Extraction order is the fix, and it is per-marker.**
The spec (§6) and ROADMAP item 18 call for trimming repeated handle tails. Measured, the doubling is an artifact of stripping bidi marks *before* extracting:

| marker | strip bidi first | extract from raw | rule |
|---|---|---|---|
| `@handle` | 212 unique on the IG sheet, **52 of them corrupted** (`almogtamarar`, `ifatglickck`, `roikaisisisis`) | 165 unique, **0 corrupted**, same 1,982 tokens | **raw only** |
| trailing byline `(שם)` | 34 found on IG | **19 found — 44% lost** to a trailing bidi mark after the `)` | **tolerate trailing bidi** |
| `#hashtag` | 12 unique / 39 tokens | 12 unique / 39 tokens | indifferent |

So: handles are matched against the raw caption, and the byline regex ends with `[bidi\s]*$`. A blanket `strip_bidi()` before tagging is a bug in both directions.

**A2. Axis 1 records `person` first and derives `program` from it.**
The dominant marker is not the hashtag the spec leads with:

| marker | IG (full captions) | TikTok |
|---|---|---|
| `@mention` | **81%** | 45% |
| trailing byline | 5% | **41%** |
| hashtag | 6% | 11% |
| any marker | **86%** | **84%** |

`person` is deterministic and needs no editorial table; `program` needs one. Writing both means a reporter missing from the program table still gets a name, and a blank `program` is legible ("nobody mapped this person") rather than ambiguous.

**A3. Storage is ~12GB/month, not ~1GB/month (§11).**
Measured: 4.6 IG reels/day (1,215 over 263 days) and 6.7 TikTok videos/day (346 over 52 days); TikTok duration median 84s but **mean 192s, p90 502s, max 1,989s** — a long tail of full programme segments. At ~2 Mbps that is ~408MB/day → **~12GB/month, ~147GB/year** (9–18GB depending on the real bitrate). Decided: a paid Google One tier on the archive-owner's account. Task 12 makes the run measure and report actual bytes so the estimate is replaced by a fact after two weeks.

---

## File Structure

| File | Responsibility |
|---|---|
| `social_dashboard/content_tags.py` (modify) | Axis 1. Pure text functions + the entity tables. Shared with the dashboard and weekly deck, as the Reshet Bet signature already is. |
| `drive_store.py` (create) | Everything that talks to the Drive API: auth from a refresh token, folder creation, resumable upload, shortcuts. Fake-able behind one class. |
| `media_archiver.py` (create) | The job: discover → filter → resolve → download → upload → classify → index, plus `--reconcile`. |
| `gdrive_consent.py` (create) | One-off, run locally by a human once, prints the refresh token. Not imported by anything. |
| `test_content_tags.py` (create) | Axis 1, including the real corrupted strings. |
| `test_drive_store.py` (create) | Folder caching, shortcut creation, upload against a fake service. |
| `test_media_archiver.py` (create) | Filter idempotency, write ordering, URL freshness, reconcile. |
| `.github/workflows/media_archiver.yml` (create) | `workflow_dispatch` only. Never `schedule:`. |
| `docs/ROADMAP.md` (modify) | Move item 18 from "designed" to "built", correct the two findings A1 and A3 record. |

---

### Task 1: Axis 1 — marker extraction

**Files:**
- Modify: `social_dashboard/content_tags.py` (append; leave the Reshet Bet section untouched)
- Test: `test_content_tags.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: `strip_bidi(text) -> str`, `extract_handles(caption) -> list[str]`, `extract_hashtags(caption) -> list[str]`, `extract_byline(caption) -> list[str]`. All are pure, no network.

- [ ] **Step 1: Write the failing test**

Create `test_content_tags.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_content_tags.py`
Expected: FAIL with `AttributeError: module 'content_tags' has no attribute 'strip_bidi'`

- [ ] **Step 3: Write minimal implementation**

Append to `social_dashboard/content_tags.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_content_tags.py`
Expected: `12 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add social_dashboard/content_tags.py test_content_tags.py
git commit -m "archive: extract caption markers in the order that does not corrupt them"
```

---

### Task 2: Axis 1 — person and program tables

**Files:**
- Modify: `social_dashboard/content_tags.py` (append below Task 1's block)
- Test: `test_content_tags.py` (append below Task 1's checks, above the summary print)

**Interfaces:**
- Consumes: `extract_handles`, `extract_hashtags`, `extract_byline` from Task 1.
- Produces: `tag_item(caption, platform) -> {"person": str, "people": list[str], "program": str, "program_source": str}`. `program_source` is one of `"hashtag"`, `"byline"`, `"mention"`, `""`. Empty strings, never `None`, because the values go straight into sheet cells.

The handle→person table below was derived by normalising every handle in the two CSVs (`re.sub(r'[._]|[0-9]+$', '', h.lower())`), which paired 28 people across the platforms automatically. The entries marked `# ידני` are the ones normalisation could not pair — reversed word order (`davidovitchsharon` / `sharondavidovitch`), a nickname (`itsik_z` / `itsikzuarets`), or a different second name (`_dorit_mizrahi` / `doritassarafmizra`).

- [ ] **Step 1: Write the failing test**

Append to `test_content_tags.py`, immediately before the `print(f"\n{PASS} passed...")` line:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_content_tags.py`
Expected: FAIL with `AttributeError: module 'content_tags' has no attribute 'tag_item'`

- [ ] **Step 3: Write minimal implementation**

Append to `social_dashboard/content_tags.py`:

```python
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
PROGRAM_BY_PERSON = {
    "יפעת גליק": "גליקותמר",
    "תמר אלמוג": "גליקותמר",
    "קרן נויבך": "סדר יום",
    "כרמלה מנשה": "רשת ב׳",
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_content_tags.py`
Expected: `27 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add social_dashboard/content_tags.py test_content_tags.py
git commit -m "archive: name the person first, derive the programme from them"
```

---

### Task 3: Drive credentials and the one-off consent script

**Files:**
- Create: `gdrive_consent.py`
- Create: `drive_store.py` (auth half only)
- Test: `test_drive_store.py` (create, auth checks only)

**Interfaces:**
- Consumes: nothing.
- Produces: `drive_store.credentials_from_env() -> google.oauth2.credentials.Credentials`, raising `RuntimeError` naming the missing variable, and `drive_store.SCOPES`. Env vars: `GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN`, and optional `GDRIVE_ROOT_FOLDER_ID`.

- [ ] **Step 1: Write the failing test**

Create `test_drive_store.py`:

```python
# -*- coding: utf-8 -*-
"""נועל את שכבת ה-Drive של ארכיון הווידאו.

ה-service account לא יכול לשמש כאן: אין לו מכסת אחסון משלו, וקובץ שהוא יוצר
בדרייב רגיל נכשל ב-storageQuotaExceeded. לכן refresh token של משתמש, ולכן
הבדיקה הזו מוודאת שחסר משתנה סביבה נופל עם שם המשתנה ולא עם KeyError סתמי.

    python test_drive_store.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import drive_store  # noqa: E402

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


print("\nשכבת Drive\n")

for var in ("GDRIVE_CLIENT_ID", "GDRIVE_CLIENT_SECRET", "GDRIVE_REFRESH_TOKEN"):
    os.environ.pop(var, None)
try:
    drive_store.credentials_from_env()
    check("חסר GDRIVE_CLIENT_ID נופל", "no error", "RuntimeError")
except RuntimeError as e:
    check("הודעת השגיאה נוקבת בשם המשתנה", "GDRIVE_CLIENT_ID" in str(e), True)

os.environ["GDRIVE_CLIENT_ID"] = "cid"
os.environ["GDRIVE_CLIENT_SECRET"] = "csec"
os.environ["GDRIVE_REFRESH_TOKEN"] = "rtok"
creds = drive_store.credentials_from_env()
check("ה-scope מוגבל ל-drive.file", list(creds.scopes),
      ["https://www.googleapis.com/auth/drive.file"])
check("ה-refresh token מגיע מהסביבה", creds.refresh_token, "rtok")

print(f"\n{PASS} passed, {FAIL} failed\n")
sys.exit(1 if FAIL else 0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_drive_store.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'drive_store'`

- [ ] **Step 3: Write minimal implementation**

Create `drive_store.py`:

```python
# -*- coding: utf-8 -*-
"""אחסון קבצי הארכיון ב-Google Drive.

**למה לא ה-service account שכל שאר הפייפליין משתמש בו:** ל-service account אין
מכסת אחסון משלו. קובץ שהוא יוצר בדרייב רגיל נכשל ב-storageQuotaExceeded, ושיתוף
תיקייה איתו לא עוזר - הקובץ שנוצר עדיין בבעלותו. שתי האפשרויות האמיתיות הן
Shared Drive (דורש Workspace) או הרשאה חד-פעמית של המשתמש שבבעלותו התיקייה.
כאן נבחרה השנייה: consent אחד מקומי (gdrive_consent.py) -> refresh token בסוד.

ה-scope הוא drive.file בלבד - גישה אך ורק לקבצים שהאפליקציה הזו יצרה. הוא
מספיק ליצירת תיקיות, העלאה וקיצורי דרך, ואינו יכול לגעת בשום דבר אחר בדרייב.

Env: GDRIVE_CLIENT_ID, GDRIVE_CLIENT_SECRET, GDRIVE_REFRESH_TOKEN,
     GDRIVE_ROOT_FOLDER_ID (אופציונלי - ברירת מחדל: שורש הדרייב).
"""

import os

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
TOKEN_URI = "https://oauth2.googleapis.com/token"

FOLDER_MIME = "application/vnd.google-apps.folder"
SHORTCUT_MIME = "application/vnd.google-apps.shortcut"


def credentials_from_env():
    """Credentials מ-refresh token. נופל בשם המשתנה החסר, לא ב-KeyError."""
    values = {}
    for var in ("GDRIVE_CLIENT_ID", "GDRIVE_CLIENT_SECRET",
                "GDRIVE_REFRESH_TOKEN"):
        v = os.environ.get(var)
        if not v:
            raise RuntimeError(f"חסר משתנה סביבה {var} - ראו gdrive_consent.py")
        values[var] = v
    return Credentials(
        token=None,
        refresh_token=values["GDRIVE_REFRESH_TOKEN"],
        client_id=values["GDRIVE_CLIENT_ID"],
        client_secret=values["GDRIVE_CLIENT_SECRET"],
        token_uri=TOKEN_URI,
        scopes=SCOPES,
    )
```

Create `gdrive_consent.py`:

```python
# -*- coding: utf-8 -*-
"""ריצה חד-פעמית, ידנית, מקומית: מפיקה את ה-GDRIVE_REFRESH_TOKEN.

לא מיובא מאף מקום ולא רץ ב-CI. דורש google-auth-oauthlib, שמותקן ידנית ו**לא**
נכנס ל-requirements.txt - הפייפליין לא צריך אותו, רק ההרשאה הראשונית.

    pip install google-auth-oauthlib
    python gdrive_consent.py client_secret.json

את client_secret.json מורידים מ-Google Cloud Console -> Credentials ->
Create OAuth client ID -> Desktop app, בפרויקט שבו Drive API מופעל.
ההרשאה חייבת להינתן בחשבון **שבבעלותו** תיקיית הארכיון.
"""

import sys

from google_auth_oauthlib.flow import InstalledAppFlow

from drive_store import SCOPES


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    flow = InstalledAppFlow.from_client_secrets_file(sys.argv[1], SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent",
                                  access_type="offline")
    if not creds.refresh_token:
        print("❌ לא הוחזר refresh token. הריצו שוב עם prompt=consent, או "
              "בטלו את ההרשאה הקיימת ב-myaccount.google.com/permissions.")
        sys.exit(1)
    print("\n✅ שמרו את שלושת אלה כסודות ב-GitHub Actions:\n")
    print(f"GDRIVE_CLIENT_ID={creds.client_id}")
    print(f"GDRIVE_CLIENT_SECRET={creds.client_secret}")
    print(f"GDRIVE_REFRESH_TOKEN={creds.refresh_token}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_drive_store.py`
Expected: `3 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add drive_store.py gdrive_consent.py test_drive_store.py
git commit -m "archive: drive credentials from a user refresh token, scoped to drive.file"
```

---

### Task 4: Drive folders, upload and shortcuts

**Files:**
- Modify: `drive_store.py`
- Test: `test_drive_store.py` (append)

**Interfaces:**
- Consumes: `credentials_from_env()` from Task 3.
- Produces: `DriveStore(service, root_id="")` with `ensure_folder(path) -> str` (path is `"2026/09/02"` or `"לפי תוכנית/גליקותמר"`, created lazily, cached per instance), `upload(local_path, name, parent_id) -> {"id": str, "bytes": int}`, `shortcut(target_id, name, parent_id) -> str`, and classmethod `from_env() -> DriveStore`.

- [ ] **Step 1: Write the failing test**

Append to `test_drive_store.py`, before the summary print:

```python
print("\nתיקיות, העלאה וקיצורים\n")


class _Exec:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class FakeFiles:
    """דרייב מזויף: סופר קריאות ומחזיר מזהים צפויים."""

    def __init__(self):
        self.created = []
        self.list_calls = 0
        self.existing = {}   # (name, parent) -> id
        self.uploads = []

    def list(self, q=None, fields=None, pageSize=None, **kw):
        self.list_calls += 1
        name = q.split("name = '")[1].split("'")[0]
        parent = q.split("'")[-2] if "in parents" in q else ""
        fid = self.existing.get((name, parent))
        return _Exec({"files": [{"id": fid}] if fid else []})

    def create(self, body=None, media_body=None, fields=None, **kw):
        fid = f"id{len(self.created) + 1}"
        self.created.append(body)
        if body.get("mimeType") == FOLDER_MIME_T:
            self.existing[(body["name"], (body.get("parents") or [""])[0])] = fid
        if media_body is not None:
            self.uploads.append(body["name"])
        return _Exec({"id": fid, "size": "1234"})


class FakeService:
    def __init__(self):
        self._files = FakeFiles()

    def files(self):
        return self._files


FOLDER_MIME_T = drive_store.FOLDER_MIME

svc = FakeService()
store = drive_store.DriveStore(svc)
a = store.ensure_folder("2026/09/02")
check("תיקייה מקוננת נוצרת לעומק", len(svc._files.created), 3)
b = store.ensure_folder("2026/09/02")
check("אותה תיקייה שנייה - מהמטמון, בלי יצירה", len(svc._files.created), 3)
check("ומחזירה את אותו מזהה", a, b)
lists_after_cache = svc._files.list_calls
store.ensure_folder("2026/09/02")
check("ואפילו בלי קריאת list", svc._files.list_calls, lists_after_cache)

import tempfile  # noqa: E402

with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as fh:
    fh.write(b"x" * 1234)
    tmp = fh.name
res = store.upload(tmp, "2026-09-02_1443_instagram_179.mp4", a)
check("ההעלאה מחזירה מזהה וגודל", (bool(res["id"]), res["bytes"]), (True, 1234))
check("הקובץ נכנס לתיקיית התאריך", svc._files.created[-1]["parents"], [a])
os.unlink(tmp)

sc = store.shortcut(res["id"], "2026-09-02_1443_instagram_179.mp4",
                    store.ensure_folder("לפי תוכנית/גליקותמר"))
check("קיצור דרך מצביע על הקובץ",
      svc._files.created[-1]["shortcutDetails"]["targetId"], res["id"])
check("וסוגו shortcut ולא עותק",
      svc._files.created[-1]["mimeType"], drive_store.SHORTCUT_MIME)
check("קיצור מוחזר עם מזהה", bool(sc), True)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_drive_store.py`
Expected: FAIL with `AttributeError: module 'drive_store' has no attribute 'DriveStore'`

- [ ] **Step 3: Write minimal implementation**

Append to `drive_store.py`:

```python
class DriveStore:
    """תיקיות, העלאות וקיצורי דרך.

    הקובץ הפיזי יושב במקום אחד בלבד - לפי תאריך. תיקיות התוכנית והקטגוריה
    מחזיקות **קיצורי דרך** אליו: פריט שייך לתוכנית וגם לקטגוריה וגם לתאריך
    בו-זמנית, קובץ לא יכול לשבת בשלוש תיקיות, וסיווג מחדש הופך להזזת מצביע
    של 3KB במקום וידאו של 40MB.

    הסייג, כי הוא מגבלה אמיתית: קיצורים מתנהגים יפה בממשק הווב של דרייב ולא
    תמיד ב-Drive for Desktop. אם יתברר שהארכיון נצרך בסנכרון תיקייה למחשב
    מקומי ולא דרך הווב או ה-API, ההחלטה הזו שווה בחינה מחדש.
    """

    def __init__(self, service, root_id=""):
        self.svc = service
        self.root_id = root_id
        self._folders = {}   # path -> id

    @classmethod
    def from_env(cls):
        creds = credentials_from_env()
        svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        return cls(svc, os.environ.get("GDRIVE_ROOT_FOLDER_ID", ""))

    def ensure_folder(self, path):
        """יוצר (או מוצא) שרשרת תיקיות. הממופה נשמר, כדי שלא נחפש בכל פריט."""
        parent = self.root_id
        walked = []
        for part in [p for p in str(path).split("/") if p]:
            walked.append(part)
            key = "/".join(walked)
            if key in self._folders:
                parent = self._folders[key]
                continue
            parent = self._folders[key] = self._find_or_create_folder(part, parent)
        return parent

    def _find_or_create_folder(self, name, parent):
        safe = name.replace("'", "\\'")
        q = f"name = '{safe}' and mimeType = '{FOLDER_MIME}' and trashed = false"
        if parent:
            q += f" and '{parent}' in parents"
        found = self.svc.files().list(
            q=q, fields="files(id)", pageSize=1).execute().get("files", [])
        if found and found[0].get("id"):
            return found[0]["id"]
        body = {"name": name, "mimeType": FOLDER_MIME}
        if parent:
            body["parents"] = [parent]
        return self.svc.files().create(body=body, fields="id").execute()["id"]

    def upload(self, local_path, name, parent_id):
        """העלאה מתחדשת (resumable) - קבצים כאן מגיעים לעשרות מגהבייט."""
        media = MediaFileUpload(local_path, mimetype="video/mp4", resumable=True)
        res = self.svc.files().create(
            body={"name": name, "parents": [parent_id]},
            media_body=media, fields="id,size").execute()
        return {"id": res["id"],
                "bytes": int(res.get("size") or os.path.getsize(local_path))}

    def shortcut(self, target_id, name, parent_id):
        return self.svc.files().create(
            body={"name": name, "mimeType": SHORTCUT_MIME,
                  "parents": [parent_id],
                  "shortcutDetails": {"targetId": target_id}},
            fields="id").execute()["id"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_drive_store.py`
Expected: `12 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add drive_store.py test_drive_store.py
git commit -m "archive: one physical file by date, shortcuts for programme and category"
```

---

### Task 5: Discovery, and the index sheet it filters against

**Files:**
- Create: `media_archiver.py`
- Test: `test_media_archiver.py` (create)

**Interfaces:**
- Consumes: `content_tags.tag_item` (Task 2).
- Produces:
  - `INDEX_SHEET`, `INDEX_HEADER` (list[str]), `ARCHIVE_LOOKBACK_HOURS`, `ARCHIVER_VERSION`, `IL_TZ`
  - `get_index(sh) -> (worksheet, set[tuple[str, str]])` — the key set is `(platform, post_id)`
  - `discover_instagram(hours) -> list[dict]`, `discover_tiktok(hours) -> list[dict]`. Each item: `{"id", "platform", "posted", "permalink", "caption", "duration_sec"}` plus `"_tiktok_urls"` on TikTok items only. **No item ever carries a resolved Instagram media URL**; TikTok's `_tiktok_urls` comes from the same aweme object already in hand and is used within the same run only.
  - `filter_new(items, known) -> list[dict]`

- [ ] **Step 1: Write the failing test**

Create `test_media_archiver.py`:

```python
# -*- coding: utf-8 -*-
"""נועל את התכונות שכל הקצב של הארכיון נשען עליהן.

  אידמפוטנטיות - ריצה שרואה פריט שכבר באינדקס לא מורידה כלום. זו הסיבה שאפשר
                  להריץ כל שעתיים בלי שהעלות תגדל: Gemini והדרייב משלמים לפי
                  פריטים שפורסמו (~11 ביום), לא לפי תדירות ריצה.
  סדר כתיבה     - שורת האינדקס אחרונה. קריסה בין ההעלאה לכתיבה עולה בקובץ
                  כפול בריצה הבאה, וזה מצב שמתאושש; ההפך - אינדקס שרשום
                  וקובץ שאינו - הופך את הארכיון לשקרן, וזה לא.
  טריות ה-URL   - media_url של אינסטגרם ו-play_addr של טיקטוק חתומים לזמן קצר.
                  URL ששמור הוא URL מת, ולכן אסור שיישמר בשום עמודה.

    python test_media_archiver.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import media_archiver as ma  # noqa: E402

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


class FakeWorksheet:
    def __init__(self, rows):
        self.rows = [list(r) for r in rows]
        self.appended = []
        self.raise_on_append = False

    def get_all_values(self):
        return [list(r) for r in self.rows]

    def append_row(self, row, value_input_option=None):
        if self.raise_on_append:
            raise RuntimeError("sheet is down")
        self.appended.append(list(row))


class FakeSpreadsheet:
    def __init__(self, ws):
        self.ws = ws

    def worksheet(self, title):
        return self.ws


print("\nאינדקס וסינון\n")

ws = FakeWorksheet([ma.INDEX_HEADER,
                    ["17912", "instagram", "2026-09-02 14:43", "", "", "f1"],
                    ["77031", "tiktok", "2026-09-02 12:10", "", "", "f2"]])
_, known = ma.get_index(FakeSpreadsheet(ws))
check("מפתח האינדקס הוא (פלטפורמה, מזהה)", known,
      {("instagram", "17912"), ("tiktok", "77031")})

items = [{"id": "17912", "platform": "instagram"},
         {"id": "77031", "platform": "tiktok"},
         {"id": "99999", "platform": "instagram"}]
check("רק החדש עובר את הסינון",
      [i["id"] for i in ma.filter_new(items, known)], ["99999"])
check("ריצה שנייה על אותו אינדקס לא משאירה כלום",
      ma.filter_new(items[:2], known), [])

check("כותרת האינדקס אינה מכילה עמודת URL של מדיה",
      [c for c in ma.INDEX_HEADER if c.endswith("_url")], [])
check("אבל כן מכילה permalink", "permalink" in ma.INDEX_HEADER, True)
check("ואת שני צירי הסיווג",
      all(c in ma.INDEX_HEADER for c in
          ("person", "program", "program_source", "category", "tags", "summary")),
      True)

print(f"\n{PASS} passed, {FAIL} failed\n")
sys.exit(1 if FAIL else 0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_media_archiver.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'media_archiver'`

- [ ] **Step 3: Write minimal implementation**

Create `media_archiver.py`:

```python
# -*- coding: utf-8 -*-
"""ארכיון וידאו - כל סרטון שכאן חדשות מפרסמת לאינסטגרם ולטיקטוק, לדרייב.

זהו אחיו של hot_sniffer.py ולא של הפייפליין היומי: ריצה תוך-יומית, גילוי ישירות
מה-API של הפלטפורמות, כתיבה **רק** לגיליון משלו. הגיליונות היומיים אינם יכולים
לשמש מקור גילוי - הקולקטורים כותבים פעם ביום ב-08:30, אז ארכיון שרץ כל שעתיים
מולם לא היה רואה דבר בין ריצה לריצה.

הסינון הוא מה שהופך את הקצב לחינמי: פריט שכבר באינדקס לא יורד שוב, אז ריצה
שלא מצאה חדש עושה קריאת API אחת לפלטפורמה ויוצאת. העלות מתקנת לפי פריטים
שפורסמו (~11 ביום), לא לפי תדירות הריצה.

**כלום לא מדודפל בזמן לכידה.** פריט יכול להגיע לטיקטוק ב-14:00 ולאינסטגרם
ב-16:00, ואי אפשר לדעת בהגעת הראשון איזה עותק "טוב יותר". כל עותק נשמר; מעבר
לילי (--reconcile) מקשר ביניהם ואינו מוחק לעולם.

Env: FACEBOOK_TOKEN, TIKHUB_TOKEN, GCP_SERVICE_ACCOUNT, GEMINI_API_KEY,
     GDRIVE_CLIENT_ID, GDRIVE_CLIENT_SECRET, GDRIVE_REFRESH_TOKEN,
     GDRIVE_ROOT_FOLDER_ID (אופציונלי).
"""

import os
import re
import sys
import json
from datetime import datetime, timedelta

import gspread
import pytz
from google.oauth2.service_account import Credentials

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "social_dashboard"))
from content_tags import tag_item, strip_bidi  # noqa: E402
from utils import http_get_json  # noqa: E402

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ACCESS_TOKEN = os.environ.get("FACEBOOK_TOKEN")
API_VERSION = "v25.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"

TIKHUB_TOKEN = os.environ.get("TIKHUB_TOKEN")
TIKTOK_USERNAME = os.environ.get("TIKTOK_USERNAME", "kan_news")
TIKTOK_SEC_UID = os.environ.get(
    "TIKTOK_SEC_UID",
    "MS4wLjABAAAA3p5tyX2Z3cacCWU34-nHbK-dpVBO5Y6IGvTj9xufL60rC6ItchtdzkEe-0frXJZX")

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
INDEX_SHEET = "ארכיון וידאו"
ARCHIVER_VERSION = "1.0"

# רחב בכוונה מ-YOUNG_HOURS=24 של הרחרחן: הרחרחן שואל "האם זה מתפוצץ עכשיו",
# שאלה עם חיי מדף קצרים; הארכיון רק צריך שריצה שהוחמצה תתאושש בבאה אחריה.
ARCHIVE_LOOKBACK_HOURS = 48

IL_TZ = pytz.timezone("Asia/Jerusalem")

INDEX_HEADER = [
    "post_id", "platform", "posted_at", "permalink", "caption",
    "drive_file_id", "drive_path", "bytes", "duration_sec",
    "person", "program", "program_source",
    "category", "tags", "summary", "credit_flag",
    "same_as", "archived_at", "archiver_version",
]


def open_spreadsheet():
    creds_json = (os.environ.get("GCP_SERVICE_ACCOUNT")
                  or os.environ.get("GOOGLE_CREDENTIALS"))
    creds_dict = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)


def get_index(sh):
    """גיליון האינדקס + סט המפתחות שכבר בו. הסט הוא כל הזיכרון של המערכת."""
    try:
        ws = sh.worksheet(INDEX_SHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=INDEX_SHEET, rows=2000,
                              cols=len(INDEX_HEADER))
        ws.append_row(INDEX_HEADER, value_input_option="RAW")
        print(f"✅ נוצר גיליון {INDEX_SHEET}")
        return ws, set()
    rows = ws.get_all_values()
    known = {(str(r[1]).strip(), str(r[0]).strip())
             for r in rows[1:] if len(r) > 1 and str(r[0]).strip()}
    return ws, known


def filter_new(items, known):
    """מה שעוד לא בארכיון. זו התכונה שכל הקצב נשען עליה."""
    return [i for i in items if (i["platform"], str(i["id"])) not in known]


def _parse_ts(ts):
    ts = re.sub(r"\+0000$", "+00:00", str(ts).replace("Z", "+00:00"))
    return datetime.fromisoformat(ts).astimezone(IL_TZ)


def discover_instagram(hours=ARCHIVE_LOOKBACK_HOURS):
    """רילסים מהחלון. media_url **לא** נשמר על הפריט - הוא נפתר בזמן ההורדה."""
    res = http_get_json(f"{BASE}/me", params={
        "access_token": ACCESS_TOKEN, "fields": "instagram_business_account"})
    ig_id = (res.get("instagram_business_account") or {}).get("id")
    if not ig_id:
        print("⚠️ לא הצלחתי לזהות את חשבון האינסטגרם")
        return []
    res = http_get_json(f"{BASE}/{ig_id}/media", params={
        "access_token": ACCESS_TOKEN,
        "fields": "id,caption,timestamp,permalink,media_type,media_product_type",
        "limit": 50,
    })
    cutoff = datetime.now(IL_TZ) - timedelta(hours=hours)
    out = []
    for m in res.get("data", []):
        if m.get("media_type") != "VIDEO":
            continue
        try:
            posted = _parse_ts(m.get("timestamp"))
        except (ValueError, TypeError):
            continue
        if posted < cutoff:
            continue
        out.append({
            "id": str(m["id"]), "platform": "instagram",
            "posted": posted, "permalink": m.get("permalink", ""),
            "caption": m.get("caption") or "", "duration_sec": "",
        })
    return out


def discover_tiktok(hours=ARCHIVE_LOOKBACK_HOURS):
    """סרטונים מהחלון. play_addr הוא העותק **בלי** הסימן - download_addr עם."""
    if not TIKHUB_TOKEN:
        print("⚠️ אין TIKHUB_TOKEN - מדלג על טיקטוק")
        return []
    res = http_get_json(
        "https://api.tikhub.io/api/v1/tiktok/app/v3/fetch_user_post_videos",
        headers={"Authorization": f"Bearer {TIKHUB_TOKEN}"},
        params={"sec_user_id": TIKTOK_SEC_UID, "max_cursor": 0,
                "count": 30, "sort_type": 0},
    )
    cutoff = datetime.now(IL_TZ) - timedelta(hours=hours)
    out = []
    for v in ((res.get("data") or {}).get("aweme_list") or []):
        ts = v.get("create_time")
        if not ts:
            continue
        posted = datetime.fromtimestamp(int(ts), tz=pytz.utc).astimezone(IL_TZ)
        if posted < cutoff:
            continue
        video = v.get("video") or {}
        urls = list(((video.get("play_addr") or {}).get("url_list")) or [])
        vid = str(v.get("aweme_id", ""))
        out.append({
            "id": vid, "platform": "tiktok", "posted": posted,
            "permalink": f"https://www.tiktok.com/@{TIKTOK_USERNAME}/video/{vid}",
            "caption": v.get("desc") or "",
            "duration_sec": round((video.get("duration") or 0) / 1000) or "",
            "_tiktok_urls": urls,
        })
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_media_archiver.py`
Expected: `6 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add media_archiver.py test_media_archiver.py
git commit -m "archive: discover from the platform APIs, filter against the index"
```

---

### Task 6: Resolve and download in one call path

**Files:**
- Modify: `media_archiver.py`
- Test: `test_media_archiver.py` (append)

**Interfaces:**
- Consumes: items from Task 5.
- Produces: `resolve_media_url(item) -> str` (Instagram: a fresh Graph call; TikTok: the first entry of `_tiktok_urls`), `http_download(url, dest, headers=None, timeout=120) -> int`, `download_media(item, dest) -> int`, `drive_filename(item) -> str`, `build_row(item, upload, drive_path, topic) -> list`. `download_media` is the only caller of `resolve_media_url`.

- [ ] **Step 1: Write the failing test**

Append to `test_media_archiver.py`, before the summary print:

```python
print("\nטריות ה-URL\n")

resolved = []
_real_resolve = ma.resolve_media_url


def spy_resolve(item):
    url = f"https://cdn.example/{item['id']}?sig=EXPIRES_SOON"
    resolved.append(url)
    return url


ma.resolve_media_url = spy_resolve
ma.http_download = lambda url, dest, **kw: (open(dest, "wb").write(b"v" * 99), 99)[1]

import tempfile  # noqa: E402

tmpdir = tempfile.mkdtemp()
dest = os.path.join(tmpdir, "x.mp4")
n = ma.download_media({"id": "17912", "platform": "instagram"}, dest)
check("ההורדה מחזירה מספר בייטים", n, 99)
check("ה-URL נפתר בתוך ההורדה ולא לפניה", len(resolved), 1)

row = ma.build_row(
    item={"id": "17912", "platform": "instagram",
          "posted": ma.datetime.now(ma.IL_TZ), "permalink": "https://ig/p/1",
          "caption": "טקסט (דב גיל-הר)", "duration_sec": 42},
    upload={"id": "drivefile1", "bytes": 99},
    drive_path="2026/09/02",
    topic={"category": "חדשות שולחן", "tags": ["בחירות 2026"],
           "summary": "שורה"})
check("שום תא בשורה אינו מכיל את ה-URL שנפתר",
      any(resolved[0] in str(c) for c in row), False)
check("השורה באורך הכותרת", len(row), len(ma.INDEX_HEADER))
check("הסיווג הדטרמיניסטי נכנס לשורה",
      row[ma.INDEX_HEADER.index("person")], "דב גיל-הר")

ma.resolve_media_url = _real_resolve
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_media_archiver.py`
Expected: FAIL with `AttributeError: module 'media_archiver' has no attribute 'resolve_media_url'`

- [ ] **Step 3: Write minimal implementation**

Append to `media_archiver.py`:

```python
# דפדפן-ish; ה-CDN של טיקטוק מחזיר 403 ל-User-Agent של requests
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

CREDIT_RE = re.compile(r"📸|סעיף 27א|צילום:|קרדיט|Reuters|AP |AFP|Getty")


def resolve_media_url(item):
    """URL טרי, ברגע ההורדה. **לעולם לא נשמר** - הוא חתום לזמן קצר ומת אחריו.

    זו הסיבה שגישת "להוסיף עמודת media_url לקולקטורים" נדחתה: היא הייתה
    מוסיפה סיכון סכמה בייצור (verify_collector.py קיים כי עמודה שנדחפת באמצע
    מזיזה כל ערך אחריה) כדי לשמור ערך שפג.
    """
    if item["platform"] == "tiktok":
        urls = item.get("_tiktok_urls") or []
        if not urls:
            raise RuntimeError("אין play_addr באובייקט ה-aweme")
        return urls[0]
    res = http_get_json(f"{BASE}/{item['id']}", params={
        "access_token": ACCESS_TOKEN, "fields": "media_url"})
    url = res.get("media_url")
    if not url:
        raise RuntimeError(f"Graph לא החזיר media_url ל-{item['id']}")
    return url


def http_download(url, dest, headers=None, timeout=120):
    """זרימה לקובץ. מוחזר מספר הבייטים שנכתבו."""
    import requests
    with requests.get(url, headers=headers, timeout=timeout, stream=True) as r:
        r.raise_for_status()
        n = 0
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)
                    n += len(chunk)
    return n


def download_media(item, dest):
    """הנתיב היחיד שמותר לו לפתור URL. 403 מנוסה שוב עם כותרות דפדפן."""
    url = resolve_media_url(item)
    try:
        return http_download(url, dest)
    except Exception as first:
        print(f"   ↻ הורדה ראשונה נכשלה ({str(first)[:80]}) - מנסה עם UA דפדפן")
        return http_download(url, dest, headers={
            "User-Agent": BROWSER_UA, "Referer": "https://www.tiktok.com/"})


def drive_filename(item):
    """תאריך, שעה, פלטפורמה ומזהה - כדי שקובץ יהיה מזוהה גם מחוץ לתיקייה שלו."""
    return (f"{item['posted'].strftime('%Y-%m-%d_%H%M')}_"
            f"{item['platform']}_{item['id']}.mp4")


def build_row(item, upload, drive_path, topic):
    """שורת אינדקס אחת. הסיווג הדטרמיניסטי ומה ש-Gemini החזיר, בשורה אחת."""
    tags = tag_item(item.get("caption"), item["platform"])
    topic = topic or {}
    return [
        str(item["id"]), item["platform"],
        item["posted"].strftime("%Y-%m-%d %H:%M"),
        item.get("permalink", ""),
        # באורך מלא - הקולקטורים קוטעים ב-500 ואיתם נעלמים קרדיטי סוף-כיתוב
        strip_bidi(item.get("caption") or ""),
        upload["id"], drive_path, upload["bytes"], item.get("duration_sec", ""),
        tags["person"], tags["program"], tags["program_source"],
        topic.get("category", ""), ", ".join(topic.get("tags") or []),
        topic.get("summary", ""),
        "כן" if CREDIT_RE.search(str(item.get("caption") or "")) else "",
        "", datetime.now(IL_TZ).strftime("%Y-%m-%d %H:%M"), ARCHIVER_VERSION,
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_media_archiver.py`
Expected: `11 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add media_archiver.py test_media_archiver.py
git commit -m "archive: resolve the media URL only inside the download that uses it"
```

---

### Task 7: Axis 2 — topic classification with Gemini

**Files:**
- Modify: `media_archiver.py`
- Test: `test_media_archiver.py` (append)

**Interfaces:**
- Consumes: `build_row` (Task 6).
- Produces: `TOPIC_CATEGORIES` (list[str]), `TOPIC_SCHEMA` (dict), `classify_topic(client, item, program) -> dict | None`. Returns `None` on failure — a failed classification must not lose the file.

- [ ] **Step 1: Write the failing test**

Append to `test_media_archiver.py`, before the summary print:

```python
print("\nציר 2 - נושא\n")


class FakeGeminiOK:
    seen = ""

    class models:
        @staticmethod
        def generate_content(model=None, contents=None, config=None):
            class R:
                text = ('{"category": "משפט ופלילים", '
                        '"tags": ["חטיפת יהלי"], "summary": "שורה אחת"}')
            FakeGeminiOK.seen = contents
            return R()


class FakeGeminiDown:
    class models:
        @staticmethod
        def generate_content(**kw):
            raise RuntimeError("503 model overloaded")


topic_item = {"id": "1", "platform": "instagram", "caption": "כיתוב הידיעה"}
got = ma.classify_topic(FakeGeminiOK, topic_item, "גליקותמר")
check("קטגוריה, תגיות וסיכום חוזרים",
      (got["category"], got["tags"], got["summary"]),
      ("משפט ופלילים", ["חטיפת יהלי"], "שורה אחת"))
check("התוכנית נכנסת לפרומפט", "גליקותמר" in FakeGeminiOK.seen, True)
check("הקטגוריות מוצעות בפרומפט", "תרבות ובידור" in FakeGeminiOK.seen, True)
check("כשל של Gemini מחזיר None ולא מתפוצץ",
      ma.classify_topic(FakeGeminiDown, topic_item, ""), None)
check("שורה עם topic=None עדיין נבנית מלאה",
      len(ma.build_row(
          item={"id": "1", "platform": "instagram",
                "posted": ma.datetime.now(ma.IL_TZ), "permalink": "",
                "caption": "כיתוב", "duration_sec": 10},
          upload={"id": "f", "bytes": 1}, drive_path="p", topic=None)),
      len(ma.INDEX_HEADER))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_media_archiver.py`
Expected: FAIL with `AttributeError: module 'media_archiver' has no attribute 'classify_topic'`

- [ ] **Step 3: Write minimal implementation**

First add one line to the import block at the top of the file, beside the other third-party imports:

```python
from google.genai import types
```

Then append to `media_archiver.py`:

```python
# רשימת פתיחה, לכוונון אחרי שיהיה פלט אמיתי.
TOPIC_CATEGORIES = [
    "חדשות שולחן", "חוץ", "צבא וביטחון", "משפט ופלילים", "כלכלה",
    "טכנולוגיה", "בריאות", "אוכל וצרכנות", "תרבות ובידור", "מגזין אנושי",
    "סאטירה",
]

TOPIC_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": TOPIC_CATEGORIES},
        "tags": {"type": "array", "items": {"type": "string"},
                 "description": "אירוע או סיפור ספציפי, למשל \"בחירות 2026\""},
        "summary": {"type": "string", "description": "שורה אחת לאינדקס"},
    },
    "required": ["category", "tags", "summary"],
}

GEMINI_MODELS = ["gemini-3.5-flash", "gemini-2.5-pro"]


def classify_topic(client, item, program):
    """קטגוריה אחת + תגיות חופשיות + סיכום. None בכשל - פריט אחד, לא הריצה.

    התגיות החופשיות הן מה שהופך את "כל מה שעלה היום על הבחירות" לשאלה שאפשר
    לענות עליה בלי שאיש חזה את הנושא מראש. הן נשמרות כפי שנכתבו - נרמול שלהן
    לאוצר מילים מבוקר הוא בכוונה מחוץ לתחום: זה נראה מסודר ומשמיד בשקט את
    הסיגנל שבגללו הן שוות משהו.
    """
    caption = strip_bidi(item.get("caption") or "")[:1500]
    program_line = ("התוכנית שזוהתה: " + program) if program else "לא זוהתה תוכנית."
    prompt = f"""אתה עורך ארכיון של חדר חדשות (כאן חדשות). לפניך כיתוב של סרטון
שפורסם ב{item['platform']}.

{program_line}

הכיתוב:
{caption}

החזר קטגוריה אחת מתוך: {" · ".join(TOPIC_CATEGORIES)}
ותגיות חופשיות שמזהות את **האירוע או הסיפור הספציפי** (למשל "בחירות 2026",
"חטיפת יהלי") - לא מילות מפתח כלליות. אם הכיתוב לא מספיק כדי לזהות סיפור,
החזר תגיות ריקות ואל תמציא.
summary: שורה אחת בעברית שמתארת מה רואים בסרטון."""

    for model_name in GEMINI_MODELS:
        try:
            res = client.models.generate_content(
                model=model_name, contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=TOPIC_SCHEMA,
                ),
            )
            return json.loads(res.text)
        except Exception as e:
            print(f"   ⚠️ {model_name} נכשל: {str(e)[:120]}")
    return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_media_archiver.py`
Expected: `16 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add media_archiver.py test_media_archiver.py
git commit -m "archive: one flash call per new item for category, free tags and a summary"
```

---

### Task 8: The pipeline, and the ordering rule it must obey

**Files:**
- Modify: `media_archiver.py`
- Test: `test_media_archiver.py` (append)

**Interfaces:**
- Consumes: everything above.
- Produces: `archive_item(item, drive, ws, client) -> list | None` and `run_archive(sh, drive, client, hours) -> (int archived, int failed)`.

- [ ] **Step 1: Write the failing test**

Append to `test_media_archiver.py`, before the summary print:

```python
print("\nסדר הכתיבה\n")


class FakeDrive:
    def __init__(self, fail_upload=False):
        self.fail_upload = fail_upload
        self.uploaded = []
        self.shortcuts = []

    def ensure_folder(self, path):
        return f"folder:{path}"

    def upload(self, local_path, name, parent_id):
        if self.fail_upload:
            raise RuntimeError("drive is down")
        self.uploaded.append(name)
        return {"id": f"drive:{name}", "bytes": 99}

    def shortcut(self, target_id, name, parent_id):
        self.shortcuts.append((target_id, parent_id))
        return "sc1"


ma.download_media = lambda item, dest: (open(dest, "wb").write(b"v" * 99), 99)[1]

base_item = {"id": "555", "platform": "instagram",
             "posted": ma.datetime.now(ma.IL_TZ), "permalink": "https://ig/p/5",
             "caption": "כיתוב #גליקותמר", "duration_sec": 30}

ws2 = FakeWorksheet([ma.INDEX_HEADER])
drive_ok = FakeDrive()
row2 = ma.archive_item(dict(base_item), drive_ok, ws2, FakeGeminiOK)
check("מסלול תקין: קובץ הועלה", len(drive_ok.uploaded), 1)
check("ושורה אחת נוספה", len(ws2.appended), 1)
check("וקיצור דרך נוצר לתיקיית התוכנית", len(drive_ok.shortcuts) >= 1, True)

ws3 = FakeWorksheet([ma.INDEX_HEADER])
drive_bad = FakeDrive(fail_upload=True)
check("דרייב שנופל: הפריט מדולג",
      ma.archive_item(dict(base_item), drive_bad, ws3, FakeGeminiOK), None)
check("ולא נשארת שורת אינדקס - הארכיון לא משקר", ws3.appended, [])

ws4 = FakeWorksheet([ma.INDEX_HEADER])
ws4.raise_on_append = True
drive_ok2 = FakeDrive()
check("גיליון שנופל: הפריט מדולג",
      ma.archive_item(dict(base_item), drive_ok2, ws4, FakeGeminiOK), None)
check("אבל הקובץ כבר בדרייב - הצד שמתאושש", len(drive_ok2.uploaded), 1)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_media_archiver.py`
Expected: FAIL with `AttributeError: module 'media_archiver' has no attribute 'archive_item'`

- [ ] **Step 3: Write minimal implementation**

First add two lines to the standard-library import block at the top of the file:

```python
import shutil
import tempfile
```

Then append to `media_archiver.py`:

```python
def archive_item(item, drive, ws, client):
    """פריט אחד, מקצה לקצה. מחזיר את השורה שנכתבה, או None אם דולג.

    **שורת האינדקס נכתבת אחרונה**, אחרי שהקובץ בדרייב. קריסה בין השתיים עולה
    בקובץ כפול בריצה הבאה, וזה מתאושש. ההפך - אינדקס שרשום וקובץ שאינו - הופך
    את הארכיון לשקרן, ואת זה אי אפשר לתקן בלי ביקורת ידנית.
    """
    tmpdir = tempfile.mkdtemp(prefix="kanarch_")
    try:
        name = drive_filename(item)
        local = os.path.join(tmpdir, name)
        download_media(item, local)

        date_path = item["posted"].strftime("%Y/%m/%d")
        upload = drive.upload(local, name, drive.ensure_folder(date_path))

        tags = tag_item(item.get("caption"), item["platform"])
        topic = classify_topic(client, item, tags["program"])

        folders = []
        if tags["program"]:
            folders.append(f"לפי תוכנית/{tags['program']}")
        if topic and topic.get("category"):
            folders.append(f"לפי קטגוריה/{topic['category']}")
        for folder in folders:
            try:
                drive.shortcut(upload["id"], name, drive.ensure_folder(folder))
            except Exception as e:   # קיצור שנכשל לא שווה איבוד הפריט
                print(f"   ⚠️ קיצור ל-{folder} נכשל: {str(e)[:100]}")

        row = build_row(item, upload, date_path, topic)
        ws.append_row(row, value_input_option="RAW")
        return row
    except Exception as e:
        print(f"   ❌ {item['platform']}/{item['id']} דולג: {str(e)[:160]}")
        return None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run_archive(sh, drive, client, hours=ARCHIVE_LOOKBACK_HOURS):
    ws, known = get_index(sh)
    print(f"📋 {len(known)} פריטים כבר בארכיון")

    found = discover_instagram(hours)
    try:
        found += discover_tiktok(hours)
    except Exception as e:   # ספק לא-רשמי, best-effort - לא מפיל את אינסטגרם
        print(f"⚠️ משיכת טיקטוק נכשלה (מדלג): {str(e)[:120]}")
    print(f"🔎 {len(found)} סרטונים בחלון של {hours} שעות")

    fresh = filter_new(found, known)
    if not fresh:
        print("ℹ️ אין חדש - הכל כבר בארכיון.")
        return 0, 0

    archived = failed = 0
    total_bytes = 0
    for item in sorted(fresh, key=lambda i: i["posted"]):
        print(f"\n--- {item['platform']} · {item['id']} · "
              f"{item['posted'].strftime('%d/%m %H:%M')} ---")
        row = archive_item(item, drive, ws, client)
        if row:
            archived += 1
            total_bytes += int(row[INDEX_HEADER.index("bytes")] or 0)
        else:
            failed += 1
    print(f"\n✅ {archived} נשמרו ({total_bytes / 1e6:,.0f}MB), {failed} דולגו")
    return archived, failed
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_media_archiver.py`
Expected: `23 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add media_archiver.py test_media_archiver.py
git commit -m "archive: the index row goes last, so the archive never lies about itself"
```

---

### Task 9: Nightly reconcile, and the false negative it is allowed to have

**Files:**
- Modify: `media_archiver.py`
- Test: `test_media_archiver.py` (append)

**Interfaces:**
- Consumes: `INDEX_HEADER`, `get_index`, `strip_bidi`.
- Produces: `caption_tokens(text, limit=40) -> frozenset[str]`, `containment(a, b) -> float`, `find_pairs(rows) -> list[tuple[int, int]]` (indices into `rows`, each row a dict with `platform`, `posted_at`, `caption`, `drive_file_id`, `same_as`), `run_reconcile(sh, days=7) -> int`.

- [ ] **Step 1: Write the failing test**

Append to `test_media_archiver.py`, before the summary print:

```python
print("\nמעבר ההצלבה\n")

# הזוג האמיתי מ-27.8: אותה ידיעה בשתי הפלטפורמות, שאינה מזווגת
ig_cap = ("תיעוד קשה מהשומרון: מתנחלים תקפו פעילי שמאל ליד הכפר בורין, "
          "שלושה נפצעו ופונו לבית החולים. המשטרה פתחה בחקירה (דב גיל-הר)")
tt_cap = "תיעוד קשה מהשומרון. עקבו אחרינו לעוד תכנים כאלה בטיקטוק"

check("הכלה של הזוג שידוע שאינו מזווג - 0.33, מתחת לסף",
      round(ma.containment(ma.caption_tokens(tt_cap),
                           ma.caption_tokens(ig_cap)), 2), 0.33)

# טיזר של שתיים-שלוש מילים דווקא **כן** מגיע ל-1.00 בהכלה, כי כל הטוקנים
# שלו מוכלים בידיעה המלאה. מה שמונע ממנו לזווג הוא RECONCILE_MIN_TOKENS,
# והמנגנון השני הזה נבדק בנפרד - אחרת נועלים ביטחון שאינו קיים.
check("טיזר קצר מגיע ל-1.00 בהכלה",
      ma.containment(ma.caption_tokens("תיעוד קשה מהשומרון"),
                     ma.caption_tokens(ig_cap)), 1.0)
check("ובכל זאת אינו מזווג - רצפת הטוקנים חוסמת אותו",
      ma.find_pairs([
          {"platform": "instagram", "posted_at": "2026-08-27 10:00",
           "caption": ig_cap, "drive_file_id": "a", "same_as": ""},
          {"platform": "tiktok", "posted_at": "2026-08-27 12:00",
           "caption": "תיעוד קשה מהשומרון", "drive_file_id": "b",
           "same_as": ""}]),
      [])

same_ig = "שר הביטחון הגיע לגבול הצפון והזהיר את חיזבאללה מפני הסלמה"
same_tt = "שר הביטחון הגיע לגבול הצפון והזהיר את חיזבאללה"
check("כיתובים כמעט זהים כן מזווגים",
      ma.containment(ma.caption_tokens(same_tt),
                     ma.caption_tokens(same_ig)) >= 0.5,
      True)

rrows = [
    {"post_id": "1", "platform": "instagram", "posted_at": "2026-09-02 14:00",
     "caption": same_ig, "drive_file_id": "figA", "same_as": ""},
    {"post_id": "2", "platform": "tiktok", "posted_at": "2026-09-02 16:00",
     "caption": same_tt, "drive_file_id": "ftkB", "same_as": ""},
    {"post_id": "3", "platform": "tiktok", "posted_at": "2026-09-02 18:00",
     "caption": "משהו אחר לגמרי על כלכלה וריבית בנק ישראל",
     "drive_file_id": "fC", "same_as": ""},
]
pairs = ma.find_pairs(rrows)
check("זוג אחד בדיוק", len(pairs), 1)
check("והוא חוצה פלטפורמות", sorted(pairs[0]), [0, 1])

check("אותה פלטפורמה לא מזווגת לעצמה",
      ma.find_pairs([rrows[1], dict(rrows[1], post_id="9")]), [])

far = [dict(rrows[0]), dict(rrows[1], posted_at="2026-09-06 16:00")]
check("הפרש של יותר מיומיים לא מזווג", ma.find_pairs(far), [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_media_archiver.py`
Expected: FAIL with `AttributeError: module 'media_archiver' has no attribute 'containment'`

- [ ] **Step 3: Write minimal implementation**

Append to `media_archiver.py`:

```python
# מילות עצירה - זהות לאלה של עמוד הוויראליות (aggregate.py:1001)
_STOP = set("""של את על עם לא זה זו זאת הוא היא הם הן אני אתם אנחנו יש אין
גם רק כל כי מה מי איך למה בין אחרי לפני נגד מול אבל או עוד כבר היום אמש מחר
כאן חדשות בעקבות במהלך בזמן כדי לפי אצל בגלל האם כמה שני שתי כמו יותר פחות
אשר כאשר היה היו תהיה הזה הזאת האלה עצמו שלו שלה שלהם ידי לאחר עקב""".split())

RECONCILE_CONTAINMENT = 0.5   # ראו הערת find_pairs
RECONCILE_WINDOW_DAYS = 2
RECONCILE_MIN_TOKENS = 4


def caption_tokens(text, limit=40):
    text = re.sub(r"[^0-9א-תa-zA-Z\s]", " ", strip_bidi(text))
    out = []
    for w in text.split():
        if len(w) >= 3 and w not in _STOP and not w.isdigit():
            out.append(w)
            if len(out) >= limit:
                break
    return frozenset(out)


def containment(a, b):
    """חפיפה ביחס לקצר מבין השניים."""
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def find_pairs(rows):
    """זוגות אינסטגרם-טיקטוק שהם אותו פריט. אף פעם לא מוחק, רק מקשר.

    הסף כאן 0.5 ולא 0.6 של עמוד הוויראליות, ומותר לו: שם רץ union-find על כל
    הפוסטים בשבוע, וסף נמוך יצר אשכול-ענק של 54 פוסטים בשרשור טרנזיטיבי; כאן
    זה זיווג 1:1 בין שתי פלטפורמות בלבד, בלי מעבר בין זוגות, אז אין מה לשרשר.

    **כשל ידוע, וזה הכיוון הבטוח:** כיתובי טיקטוק הם טיזרים וכיתובי אינסטגרם
    הם ידיעה מלאה, אז פריטים זהים באמת אינם מזווגים - "תיעוד קשה מהשומרון"
    עם זנב הקריאה-לפעולה שלו קיבל 0.33 מול פוסט האינסטגרם של עצמו (נמדד).
    שימו לב שהמנגנון תלוי באורך הטיזר ולא ברור מאליו: טיזר של שלוש מילים
    בלבד מגיע דווקא ל-1.00, כי כל הטוקנים שלו מוכלים בידיעה המלאה, ומה שמונע
    ממנו לזווג הוא RECONCILE_MIN_TOKENS. שני המסלולים נבדקים בנפרד.

    הכיוון הזה של הטעות הוא הבטוח: זוג שלא זווג משאיר שני קבצים בארכיון,
    בעוד זיווג שגוי היה מסתיר תוכן אמיתי מאחורי סימון כפילות.
    """
    prepped = []
    for i, r in enumerate(rows):
        toks = caption_tokens(r.get("caption", ""))
        if len(toks) < RECONCILE_MIN_TOKENS:
            continue
        try:
            d = datetime.strptime(str(r.get("posted_at", ""))[:10], "%Y-%m-%d")
        except ValueError:
            continue
        prepped.append((i, r.get("platform", ""), d, toks))

    used, pairs = set(), []
    for ia, pa, da, ta in prepped:
        if ia in used:
            continue
        best, best_score = None, 0.0
        for ib, pb, db, tb in prepped:
            if ib in used or ib == ia or pb == pa:
                continue
            if abs((da - db).days) > RECONCILE_WINDOW_DAYS:
                continue
            s = containment(ta, tb)
            if s >= RECONCILE_CONTAINMENT and s > best_score:
                best, best_score = ib, s
        if best is not None:
            used.update({ia, best})
            pairs.append((ia, best))
    return pairs


def run_reconcile(sh, days=7):
    """מקשר עותקים ומדפיס את דוח הפערים - מה קיים בפלטפורמה אחת ולא בשנייה."""
    ws, _ = get_index(sh)
    values = ws.get_all_values()
    if len(values) < 2:
        print("ℹ️ האינדקס ריק.")
        return 0
    header, body = values[0], values[1:]
    cutoff = (datetime.now(IL_TZ) - timedelta(days=days)).strftime("%Y-%m-%d")
    rows, row_numbers = [], []
    for n, raw in enumerate(body, start=2):
        r = dict(zip(header, list(raw) + [""] * (len(header) - len(raw))))
        if str(r.get("posted_at", ""))[:10] >= cutoff:
            rows.append(r)
            row_numbers.append(n)

    same_col = header.index("same_as") + 1
    pairs = find_pairs(rows)
    updates = 0
    for i, j in pairs:
        for src, dst in ((i, j), (j, i)):
            if not rows[src].get("same_as"):
                ws.update_cell(row_numbers[src], same_col,
                               rows[dst].get("drive_file_id", ""))
                updates += 1

    linked = {i for p in pairs for i in p}
    only = {"instagram": [], "tiktok": []}
    for idx, r in enumerate(rows):
        if idx not in linked and r.get("platform") in only:
            only[r["platform"]].append(r)
    print(f"🔗 {len(pairs)} זוגות קושרו ({updates} תאים עודכנו)")
    print(f"📊 דוח פערים ל-{days} הימים האחרונים: "
          f"{len(only['tiktok'])} רק בטיקטוק, "
          f"{len(only['instagram'])} רק באינסטגרם")
    for plat, gap_items in only.items():
        for r in gap_items[:15]:
            print(f"   [{plat}] {r.get('posted_at', '')} "
                  f"{r.get('caption', '')[:70]}")
    return len(pairs)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_media_archiver.py`
Expected: `31 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add media_archiver.py test_media_archiver.py
git commit -m "archive: link the two copies nightly, and lock the false negative in a test"
```

---

### Task 10: CLI entry point

**Files:**
- Modify: `media_archiver.py`
- Test: `test_media_archiver.py` (append)

**Interfaces:**
- Consumes: `run_archive`, `run_reconcile`, `get_index`, `discover_*`.
- Produces: `parse_args(argv=None)` returning a namespace with `hours`, `reconcile`, `dry_run`, and `main()`.

- [ ] **Step 1: Write the failing test**

Append to `test_media_archiver.py`, before the summary print:

```python
print("\nממשק שורת הפקודה\n")

check("--since-days מתורגם לשעות",
      ma.parse_args(["--since-days", "3"]).hours, 72)
check("ברירת המחדל היא חלון הארכיון",
      ma.parse_args([]).hours, ma.ARCHIVE_LOOKBACK_HOURS)
check("--reconcile מזוהה", ma.parse_args(["--reconcile"]).reconcile, True)
check("--dry-run מזוהה", ma.parse_args(["--dry-run"]).dry_run, True)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_media_archiver.py`
Expected: FAIL with `AttributeError: module 'media_archiver' has no attribute 'parse_args'`

- [ ] **Step 3: Write minimal implementation**

First add these to the import block at the top of the file — `argparse` with the standard library, the other two beside the existing third-party and local imports:

```python
import argparse
from google import genai
import drive_store  # noqa: E402
```

Then append to `media_archiver.py`:

```python
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="ארכיון וידאו - אינסטגרם וטיקטוק")
    p.add_argument("--since-days", type=int, default=None,
                   help="לחזור כמה ימים אחורה במקום 48 שעות")
    p.add_argument("--reconcile", action="store_true",
                   help="מעבר ההצלבה הלילי במקום ארכוב")
    p.add_argument("--dry-run", action="store_true",
                   help="לגלות ולסנן בלבד - בלי הורדה, העלאה או כתיבה")
    args = p.parse_args(argv)
    args.hours = (args.since_days * 24 if args.since_days
                  else ARCHIVE_LOOKBACK_HOURS)
    return args


def main():
    args = parse_args()
    now = datetime.now(IL_TZ)
    print(f"\n🎬 ארכיון וידאו - {now.strftime('%Y-%m-%d %H:%M')}\n")

    if not ACCESS_TOKEN:
        print("❌ חסר FACEBOOK_TOKEN")
        sys.exit(1)

    sh = open_spreadsheet()

    if args.reconcile:
        run_reconcile(sh)
        return

    if args.dry_run:
        _, known = get_index(sh)
        found = discover_instagram(args.hours)
        try:
            found += discover_tiktok(args.hours)
        except Exception as e:
            print(f"⚠️ משיכת טיקטוק נכשלה: {str(e)[:120]}")
        fresh = filter_new(found, known)
        print(f"\n🧪 מצב יבש: {len(found)} בחלון, {len(fresh)} חדשים")
        for i in fresh:
            print(f"   {i['platform']:10s} {i['id']:20s} "
                  f"{i['posted'].strftime('%d/%m %H:%M')}  "
                  f"{strip_bidi(i.get('caption', ''))[:60]}")
        return

    # דרייב שלא נגיש מפיל את הריצה בכוונה - הוורקפלואו צריך להאדים
    drive = drive_store.DriveStore.from_env()
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    archived, failed = run_archive(sh, drive, client, args.hours)
    if archived == 0 and failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_media_archiver.py`
Expected: `35 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add media_archiver.py test_media_archiver.py
git commit -m "archive: --since-days, --reconcile and a dry run that touches nothing"
```

---

### Task 11: Report the real storage cost from the first runs

**Files:**
- Modify: `media_archiver.py`
- Test: `test_media_archiver.py` (append)

**Interfaces:**
- Consumes: `run_reconcile`'s `rows`.
- Produces: `storage_report(rows) -> {"total_bytes", "days", "per_day_mb", "projected_gb_month"}`. Called at the end of `run_reconcile` so the nightly log carries it.

This exists because §11's estimate was out by an order of magnitude and the plan's replacement (A3) is also an estimate. After two weeks the log holds the fact instead.

- [ ] **Step 1: Write the failing test**

Append to `test_media_archiver.py`, before the summary print:

```python
print("\nדוח אחסון\n")

srows = [{"posted_at": "2026-09-01 10:00", "bytes": "50000000"},
         {"posted_at": "2026-09-01 12:00", "bytes": "30000000"},
         {"posted_at": "2026-09-03 12:00", "bytes": "20000000"}]
rep = ma.storage_report(srows)
check("סך הבייטים", rep["total_bytes"], 100000000)
check("שני ימים קלנדריים שונים", rep["days"], 2)
check("ממוצע יומי במגה", round(rep["per_day_mb"]), 50)
check("תחזית חודשית בג'יגה", round(rep["projected_gb_month"], 1), 1.5)
check("אינדקס ריק לא מחלק באפס", ma.storage_report([])["per_day_mb"], 0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python test_media_archiver.py`
Expected: FAIL with `AttributeError: module 'media_archiver' has no attribute 'storage_report'`

- [ ] **Step 3: Write minimal implementation**

Append to `media_archiver.py`:

```python
def storage_report(rows):
    """כמה הארכיון באמת שוקל. ההערכה במפרט הייתה 1GB לחודש והמדידה מהכיתובים
    שעל הדיסק אמרה ~12 - אז הריצה מודדת במקום להעריך, ואחרי שבועיים יש עובדה
    בלוג במקום שתי הערכות."""
    total = 0
    days = set()
    for r in rows:
        try:
            total += int(str(r.get("bytes") or 0).strip() or 0)
        except ValueError:
            pass
        d = str(r.get("posted_at", ""))[:10]
        if d:
            days.add(d)
    n = len(days)
    per_day = (total / n / 1e6) if n else 0
    return {"total_bytes": total, "days": n, "per_day_mb": per_day,
            "projected_gb_month": per_day * 30 / 1000}
```

At the end of `run_reconcile`, immediately before `return len(pairs)`, add:

```python
    rep = storage_report(rows)
    print(f"💾 {rep['total_bytes'] / 1e9:.2f}GB על פני {rep['days']} ימים = "
          f"{rep['per_day_mb']:,.0f}MB ליום → "
          f"{rep['projected_gb_month']:.1f}GB לחודש")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python test_media_archiver.py`
Expected: `40 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add media_archiver.py test_media_archiver.py
git commit -m "archive: measure the storage instead of estimating it"
```

---

### Task 12: Workflow, and the roadmap entry it closes

**Files:**
- Create: `.github/workflows/media_archiver.yml`
- Modify: `docs/ROADMAP.md`

**Interfaces:**
- Consumes: `media_archiver.py` CLI (Task 10).
- Produces: a `workflow_dispatch`-only workflow. **No `schedule:`** — GitHub's cron lags 4–6 hours; the trigger is a VPS systemd timer, as with `kan-hot-sniffer.timer` (`hot_sniffer.yml:4`).

- [ ] **Step 1: Write the workflow**

Create `.github/workflows/media_archiver.yml`:

```yaml
name: Media Archiver

on:
  workflow_dispatch:      # הטריגר: טיימר על ה-VPS (kan-media-archiver.timer), כמו
                          # הרחרחן. בכוונה בלי schedule: - ה-cron של GitHub מפגר בשעות.
    inputs:
      since_days:
        description: 'לחזור כמה ימים אחורה במקום 48 שעות'
        required: false
        default: ''
      reconcile:
        description: 'מעבר ההצלבה במקום ארכוב (כל ערך = דלוק)'
        required: false
        default: ''
      dry_run:
        description: 'גילוי וסינון בלבד, בלי הורדה או כתיבה (כל ערך = דלוק)'
        required: false
        default: ''

jobs:
  archive:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run Media Archiver
        env:
          FACEBOOK_TOKEN: ${{ secrets.FACEBOOK_TOKEN }}
          TIKHUB_TOKEN: ${{ secrets.TIKHUB_TOKEN }}
          GCP_SERVICE_ACCOUNT: ${{ secrets.GCP_SERVICE_ACCOUNT }}
          GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}
          GDRIVE_CLIENT_ID: ${{ secrets.GDRIVE_CLIENT_ID }}
          GDRIVE_CLIENT_SECRET: ${{ secrets.GDRIVE_CLIENT_SECRET }}
          GDRIVE_REFRESH_TOKEN: ${{ secrets.GDRIVE_REFRESH_TOKEN }}
          GDRIVE_ROOT_FOLDER_ID: ${{ secrets.GDRIVE_ROOT_FOLDER_ID }}
        run: |
          ARGS=""
          if [ -n "${{ inputs.since_days }}" ]; then ARGS="$ARGS --since-days ${{ inputs.since_days }}"; fi
          if [ -n "${{ inputs.reconcile }}" ]; then ARGS="$ARGS --reconcile"; fi
          if [ -n "${{ inputs.dry_run }}" ]; then ARGS="$ARGS --dry-run"; fi
          python media_archiver.py $ARGS
```

- [ ] **Step 2: Verify the workflow parses**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/media_archiver.yml', encoding='utf-8')); print('ok')"`
Expected: `ok`

- [ ] **Step 3: Update the roadmap**

In `docs/ROADMAP.md`, replace the whole `## Open — video archive` section (item 18) with:

```markdown
## Open — video archive

### 18. The archive is built; the first live run has not been made (2026-09-04)
`media_archiver.py`, `drive_store.py`, `social_dashboard/content_tags.py`
(axis 1) and `.github/workflows/media_archiver.yml`, implementing
`docs/superpowers/specs/2026-09-02-video-archive-design.md` through the plan at
`docs/superpowers/plans/2026-09-04-video-archive.md`.

Before it can be trusted, three things must happen and none of them is code:

1. **`gdrive_consent.py` must be run once**, by the account that owns the
   archive folder, and its three values stored as GitHub secrets. Until then
   every run fails at `DriveStore.from_env()`.
2. **`python media_archiver.py --since-days 2` once**, then open the Drive
   folder: confirm the files play, are not watermarked, and that their count
   matches the index. The no-watermark claim rests on `play_addr` rather than
   `download_addr` and has never been checked against our own account.
3. **A systemd timer on the VPS** firing `workflow_dispatch` every two hours,
   alongside `kan-hot-sniffer.timer`, plus a daily one with `reconcile=1`.

Two of the design's own numbers were wrong and are corrected in the plan's
"Amendments" section:

- **Storage is ~12GB/month, not ~1GB.** 4.6 IG reels/day and 6.7 TikTok
  videos/day, and TikTok's duration is median 84s but **mean 192s, p90 502s,
  max 1,989s** — a long tail of whole programme segments. ~147GB/year at
  ~2 Mbps. A paid Google One tier was chosen over a length cap, because the
  long items are exactly the programme segments the story rails are for.
  `storage_report()` prints the measured figure on every reconcile run, so the
  estimate is replaced by a fact after two weeks.
- **The Instagram handle corruption needs no "doubled-tail trim" — it needs
  the right extraction order,** and the order differs per marker. Handles must
  be read from the **raw** caption (stripping bidi first invents 52 corrupt
  handles out of 212; reading raw gives 165 and none, over the same 1,982
  tokens). The byline regex must **tolerate** bidi after the closing bracket
  (without it, 19 bylines are found on Instagram instead of 34 — 44% lost).
  Hashtags are indifferent. `test_content_tags.py` locks all three.

A third finding from the design work still stands on its own: **a hashtag means
"programme segment" at ~99% precision but names the programme only 84% of the
time, and reaches just 9% of items.** Counting programme output by hashtag
undercounts ~3× — גליקותמר 3.4/week by tag against 5.7 by byline. Measured
again over the caption files on disk, the marker that actually carries the
coverage is the **@mention** (81% of full IG captions, 45% of TikTok) with the
trailing byline behind it (5% IG, 41% TikTok); together with hashtags they
cover 86% of Instagram and 84% of TikTok. This is why axis 1 writes `person`
as its own column and derives `program` from it: `PROGRAM_BY_PERSON` is
deliberately partial, and a reporter missing from it still gets a name.
```

- [ ] **Step 4: Run the whole suite**

Run: `python test_content_tags.py && python test_drive_store.py && python test_media_archiver.py`
Expected: all three print `0 failed` and exit 0.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/media_archiver.yml docs/ROADMAP.md
git commit -m "archive: dispatch-only workflow, and the roadmap item it closes"
```

---

## After the plan: what a human must do

Code alone does not make this run. In order:

1. Google Cloud Console → the project that already holds the Sheets service account → enable **Drive API** → Credentials → **OAuth client ID → Desktop app** → download `client_secret.json`.
2. `pip install google-auth-oauthlib && python gdrive_consent.py client_secret.json`, consenting **as the account that owns the archive folder**. Store the three printed values as GitHub secrets.
3. Create the root folder `כאן חדשות — ארכיון וידאו` in that Drive and put its id in `GDRIVE_ROOT_FOLDER_ID` (optional; without it everything lands at the Drive root).
4. Upgrade that account's Google One tier — see amendment A3 for the numbers.
5. `gh workflow run media_archiver.yml -f dry_run=1` — confirm discovery finds today's items and the count looks right, before anything is written.
6. `gh workflow run media_archiver.yml -f since_days=2` — then **open the Drive folder**: files play, no watermark, count matches the index.
7. Add `kan-media-archiver.timer` on the VPS, every two hours, plus a daily `reconcile=1` fire.

## Self-review notes

- Spec sections map to tasks: §2/§3 → Tasks 5, 6, 8; §4 → Task 8; §5 → Task 4; §6 → Tasks 1, 2, 7; §7 → Task 3; §8 → Tasks 5, 6; §9 → Task 9; §10 → the tests inside each task; §11 → Task 11; §12 stays out of scope.
- §10's "byline tagger must resolve `ifatglickckck`" is deliberately **not** implemented. Amendment A1 replaces it: the corrupted string is never produced, and Task 1's test asserts that instead.
- `tag_item` returns `people` as well as `person`; only `person` reaches the sheet. The list is there for the dashboard, which shares this module.
- `PROGRAM_BY_PERSON` ships with four entries on purpose. It is the one table that needs editorial knowledge the caption data cannot supply, and it is designed to be extended a row at a time without touching code.
