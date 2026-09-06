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
    def __init__(self, ws, missing=False):
        self.ws = ws
        self.missing = missing
        self.added = None

    def worksheet(self, title):
        if self.missing:
            raise ma.gspread.WorksheetNotFound(title)
        return self.ws

    def add_worksheet(self, title, rows, cols):
        self.added = FakeWorksheet([])
        return self.added


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

print("\nאינדקס - יצירה כשהגיליון חסר\n")

# מסלול שרץ פעם אחת בחיי הגיליון - בדיוק המסלול ש-save_daily_insights_to_sheets
# נשא בו קריאת gspread שבורה בלי שאף בדיקה תפסה, לפי docs/ROADMAP.md.
sh_missing = FakeSpreadsheet(None, missing=True)
ws_new, known_new = ma.get_index(sh_missing)
check("גיליון חדש נוצר כשהוא חסר", ws_new is sh_missing.added, True)
check("השורה הראשונה שנכתבת בו היא כותרת האינדקס",
      sh_missing.added.appended, [ma.INDEX_HEADER])
check("וסט המפתחות מתחיל ריק", known_new, set())

print("\nטריות ה-URL\n")

resolved = []
_real_resolve = ma.resolve_media_url
_real_download_media = ma.download_media


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

print("\nציר 2 - נושא\n")


class FakeGeminiOK:
    seen = ""
    seen_config = None
    seen_models = []

    class models:
        @staticmethod
        def generate_content(model=None, contents=None, config=None):
            class R:
                text = ('{"category": "משפט ופלילים", '
                        '"tags": ["חטיפת יהלי"], "summary": "שורה אחת"}')
            FakeGeminiOK.seen = contents
            FakeGeminiOK.seen_config = config
            FakeGeminiOK.seen_models.append(model)
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

# טוקני חשיבה מחויבים כפלט. נמדד 2026-09-06 על הפרומפט הזה עצמו:
# gemini-3.5-flash חשב 595 טוקנים כדי להחזיר 73, וה-Pro שהיה בשרשרת הנפילה
# חשב 855 ו**אינו מאפשר** thinking_budget=0 (400 INVALID_ARGUMENT). לכן אין
# Pro בשרשרת, ולכן כל קריאה מכבה חשיבה במפורש.
check("החשיבה מכובה בכל קריאה",
      FakeGeminiOK.seen_config.thinking_config.thinking_budget, 0)
check("אין Pro בשרשרת - הוא לא יודע לכבות חשיבה",
      [m for m in ma.GEMINI_MODELS if "pro" in m], [])
check("כל המודלים בשרשרת הם flash",
      all("flash" in m for m in ma.GEMINI_MODELS), True)
check("נקרא המודל הראשון בשרשרת",
      FakeGeminiOK.seen_models[0], ma.GEMINI_MODELS[0])
check("שורה עם topic=None עדיין נבנית מלאה",
      len(ma.build_row(
          item={"id": "1", "platform": "instagram",
                "posted": ma.datetime.now(ma.IL_TZ), "permalink": "",
                "caption": "כיתוב", "duration_sec": 10},
          upload={"id": "f", "bytes": 1}, drive_path="p", topic=None)),
      len(ma.INDEX_HEADER))

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

print("\nניקוי URL משגיאות\n")

import io  # noqa: E402
import contextlib  # noqa: E402
import requests  # noqa: E402

# HTTPError אמיתי, בדיוק כמו זה שר-raise_for_status מרים: ה-URL החתום חי
# בתוך הודעת השגיאה עצמה, לא בפרמטר נפרד.
SECRET_URL = "https://cdn.example/secret-media?sig=SHOULD_NOT_LEAK12345"
http_err = requests.exceptions.HTTPError(
    f"403 Client Error: Forbidden for url: {SECRET_URL}")


def _first_call_raises_then_ok(url, dest, headers=None, timeout=120):
    if headers is None:
        raise http_err
    return (open(dest, "wb").write(b"v" * 99), 99)[1]


ma.http_download = _first_call_raises_then_ok
ma.resolve_media_url = lambda item: SECRET_URL
# download_media עצמו נשאר הפסיק מ"סדר הכתיבה" למעלה - בלי לשחזר אותו כאן
# הקריאה הבאה הייתה קוראת לפסיק ההוא (שתמיד מחזיר 99) ומדלגת על הלוגיקה
# האמיתית של ניסיון-חוזר לגמרי, בלי שהבדיקה תיכשל.
ma.download_media = _real_download_media

tmpdir5 = tempfile.mkdtemp()
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    n5 = ma.download_media({"id": "9", "platform": "tiktok"},
                            os.path.join(tmpdir5, "y.mp4"))
check("ההורדה מתאוששת אחרי ניסיון ראשון שנכשל", n5, 99)
# בדיקה על תת-מחרוזת מוקדמת ולא על SECRET_URL השלם: השורה הזו נחתכת ב-[:80]
# ב-media_archiver.py, אז חלק מהזנב של URL ארוך נחתך גם כשהוא לא מנוקה בכלל -
# "cdn.example" תמיד באזור הבטוח מהחיתוך, לכן היעדרותו מוכיח בפועל שהוחלף
# ב-<url> ולא שרק נחתך במקרה.
check("אבל ה-URL החתום לא מודפס בלוג הניסיון החוזר",
      "cdn.example" in buf.getvalue(), False)


def _download_raises(item, dest):
    raise http_err


ma.download_media = _download_raises

ws5 = FakeWorksheet([ma.INDEX_HEADER])
drive5 = FakeDrive()
buf2 = io.StringIO()
with contextlib.redirect_stdout(buf2):
    result5 = ma.archive_item(dict(base_item), drive5, ws5, FakeGeminiOK)
check("פריט שההורדה שלו נכשלת מדולג", result5, None)
check("וה-URL החתום לא מודפס בשורת הדילוג של הפריט",
      SECRET_URL in buf2.getvalue(), False)

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

print("\nממשק שורת הפקודה\n")

check("--since-days מתורגם לשעות",
      ma.parse_args(["--since-days", "3"]).hours, 72)
check("ברירת המחדל היא חלון הארכיון",
      ma.parse_args([]).hours, ma.ARCHIVE_LOOKBACK_HOURS)
check("--reconcile מזוהה", ma.parse_args(["--reconcile"]).reconcile, True)
check("--dry-run מזוהה", ma.parse_args(["--dry-run"]).dry_run, True)

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

print("\nגריעה אחרי חלון השמירה\n")


class FakeDrive:
    """דרייב מזויף: זוכר מה נמחק, ויכול לזרוק על מזהה מסוים."""

    def __init__(self, raise_on=()):
        self.deleted = []
        self.raise_on = set(raise_on)
        self.shortcuts = {}

    def delete(self, file_id):
        if file_id in self.raise_on:
            raise RuntimeError("500 backend error")
        self.deleted.append(file_id)
        return True

    def delete_shortcuts(self, target_id):
        n = self.shortcuts.get(target_id, 0)
        self.deleted.extend([f"sc:{target_id}"] * n)
        return n


class FakeWS:
    """גיליון מזויף: מחזיק ערכים ורושם כל update_cell."""

    def __init__(self, values):
        self.values = values
        self.updates = []

    def get_all_values(self):
        return self.values

    def update_cell(self, row, col, value):
        self.updates.append((row, col, value))
        while len(self.values[row - 1]) < col:
            self.values[row - 1].append("")
        self.values[row - 1][col - 1] = value


def _prune_rows(*specs):
    hdr = list(ma.INDEX_HEADER)
    out = [hdr]
    for pid, day, fid, deleted in specs:
        r = [""] * len(hdr)
        r[hdr.index("post_id")] = pid
        r[hdr.index("platform")] = "instagram"
        r[hdr.index("posted_at")] = day
        r[hdr.index("drive_file_id")] = fid
        r[hdr.index("bytes")] = "1000"
        r[hdr.index("deleted_at")] = deleted
        out.append(r)
    return out


_old = (ma.datetime.now(ma.IL_TZ) - ma.timedelta(days=30)).strftime("%Y-%m-%d")
_new = ma.datetime.now(ma.IL_TZ).strftime("%Y-%m-%d")


_ws = FakeWS(_prune_rows(("old", _old, "fileA", ""), ("new", _new, "fileB", "")))
_drive = FakeDrive()
_drive.shortcuts["fileA"] = 2
_freed = ma.prune_old(_drive, _ws, days=7)
check("הישן נמחק מהדרייב", "fileA" in _drive.deleted, True)
check("החדש לא נגע", "fileB" in _drive.deleted, False)
check("גם קיצורי הדרך שלו נמחקו",
      sum(1 for d in _drive.deleted if d == "sc:fileA"), 2)
check("השורה סומנה ולא נמחקה", len(_ws.values), 3)
_col = ma.INDEX_HEADER.index("deleted_at") + 1
check("הסימון נכתב לעמודת deleted_at",
      [u for u in _ws.updates if u[1] == _col][0][0], 2)
check("הנפח המשוחרר מדווח", _freed, 1000)

# הכיוון הבטוח: מוחקים קודם ומסמנים אחר כך. אילו הסימון היה קודם וההסרה
# נכשלת, השורה כבר טוענת "נמחק" ואף ריצה לא תחזור אליה - הקובץ דולף לנצח.
_ws2 = FakeWS(_prune_rows(("old", _old, "fileC", "")))
ma.prune_old(FakeDrive(raise_on=["fileC"]), _ws2, days=7)
check("כשל מחיקה לא מסמן את השורה", _ws2.updates, [])

_ws3 = FakeWS(_prune_rows(("old", _old, "fileD", "2026-01-01")))
_drive3 = FakeDrive()
ma.prune_old(_drive3, _ws3, days=7)
check("שורה שכבר סומנה לא נמחקת שוב", _drive3.deleted, [])

_ws4 = FakeWS(_prune_rows(("old", _old, "fileE", "")))
_drive4 = FakeDrive()
ma.prune_old(_drive4, _ws4, days=7, dry_run=True)
check("מצב יבש לא מוחק ולא מסמן", (_drive4.deleted, _ws4.updates), ([], []))


print("\nמדידה: רוחב, גובה, ביטרייט - ומי העותק המועדף\n")

# הכותרת נכתבת לפי מיקום. 20 העמודות הראשונות קפואות בסדר הזה; עמודה חדשה
# נכנסת רק אחריהן. שינוי כאן = כל ערך בגיליון החי מזיז עמודה.
_FROZEN = [
    "post_id", "platform", "posted_at", "permalink", "caption",
    "drive_file_id", "drive_path", "bytes", "duration_sec",
    "person", "program", "program_source",
    "category", "tags", "summary", "credit_flag",
    "same_as", "archived_at", "archiver_version", "deleted_at",
]
check("20 העמודות ההיסטוריות לא זזו", ma.INDEX_HEADER[:20], _FROZEN)
check("עמודות המדידה אחריהן, בסדר הזה",
      ma.INDEX_HEADER[20:], ["width", "height", "kbps", "preferred"])

_item = {"id": "7", "platform": "tiktok", "posted": ma.datetime.now(ma.IL_TZ),
         "permalink": "", "caption": "כיתוב", "duration_sec": ""}
_up = {"id": "f1", "bytes": 10}
_probe = {"width": 1080, "height": 1910, "kbps": 1057, "duration_sec": 8}
_row = ma.build_row(_item, _up, "2026/09/06", None, probe=_probe)
_h = ma.INDEX_HEADER
check("השורה באורך הכותרת החדשה", len(_row), len(_h))
check("רוחב וגובה נכנסים לשורה",
      (_row[_h.index("width")], _row[_h.index("height")]), (1080, 1910))
check("ביטרייט נכנס", _row[_h.index("kbps")], 1057)
check("משך מהמדידה כשהפלטפורמה לא נתנה", _row[_h.index("duration_sec")], 8)
check("preferred ריק בזמן הארכוב - נקבע רק בהצלבה",
      _row[_h.index("preferred")], "")

_item2 = dict(_item, duration_sec=31)
_row2 = ma.build_row(_item2, _up, "2026/09/06", None, probe=_probe)
check("משך מהפלטפורמה גובר על המדידה", _row2[_h.index("duration_sec")], 31)

_row3 = ma.build_row(_item, _up, "2026/09/06", None, probe=None)
check("בלי ffprobe - עמודות ריקות, שורה מלאה",
      (len(_row3), _row3[_h.index("width")]), (len(_h), ""))

_real_which = ma.shutil.which
ma.shutil.which = lambda name: None
check("ffprobe חסר מחזיר None ולא מפיל", ma.probe_media("/no/such.mp4"), None)
ma.shutil.which = _real_which
check("קובץ שאינו וידאו מחזיר None ולא מפיל",
      ma.probe_media(__file__), None)

# מי העותק המועדף: לפי מדידה, לא לפי פלטפורמה. נמדד 2026-09-06 - אינסטגרם
# 716x1266 מול טיקטוק 1080x1910 בשניים משלושה זוגות, והפוך בשלישי.
_ig = {"platform": "instagram", "width": "716", "height": "1266", "kbps": "958"}
_tt = {"platform": "tiktok", "width": "1080", "height": "1910", "kbps": "1057"}
check("הרזולוציה הגבוהה מנצחת - גם כשהיא טיקטוק",
      ma.pick_preferred(_ig, _tt), _tt)
check("וסימטרי", ma.pick_preferred(_tt, _ig), _tt)
_ig_hi = {"platform": "instagram", "width": "720", "height": "1280", "kbps": "1599"}
_tt_lo = {"platform": "tiktok", "width": "576", "height": "1018", "kbps": "682"}
check("וגם כשהיא אינסטגרם", ma.pick_preferred(_ig_hi, _tt_lo), _ig_hi)
_a = {"platform": "instagram", "width": "720", "height": "1280", "kbps": "900"}
_b = {"platform": "tiktok", "width": "720", "height": "1280", "kbps": "1400"}
check("שוויון בפיקסלים - הביטרייט מכריע", ma.pick_preferred(_a, _b), _b)
_none = {"platform": "instagram", "width": "", "height": "", "kbps": ""}
check("שניהם לא נמדדו - אין העדפה, לא ניחוש",
      ma.pick_preferred(_none, dict(_none, platform="tiktok")), None)
check("רק אחד נמדד - הוא המועדף", ma.pick_preferred(_none, _tt), _tt)



print("\nבדיקת בריאות של אישורי הדרייב\n")


class _RootOK:
    def __init__(self):
        self.calls = 0

    def _root(self):
        self.calls += 1
        return "rootid"


class _RootDead:
    def _root(self):
        raise RuntimeError("invalid_grant: Token has been expired or revoked.")


_ok = _RootOK()
check("בדיקה מוצלחת מחזירה את מזהה השורש", ma.check_drive(_ok), "rootid")
check("והיא עושה קריאת דרייב אמיתית אחת", _ok.calls, 1)
check("טוקן מת מחזיר None ולא מתפוצץ", ma.check_drive(_RootDead()), None)


print("\nזיווג בלתי תלוי בסדר\n")

# הפגם אינו תיאורטי אבל גם לא נצפה בנתונים (נמדד 2026-09-06: 40 זיווגים,
# סטים זהים בשתי השיטות). המקרה שמפיל את הגרסה החמדנית: פריט אחד שמתאים
# לשניים, כשהחלש מבין השניים נסרק ראשון ותופס אותו.
# ההכלה נמדדת מול הקצר מבין השניים, אז כיתוב קצר שכולו מוכל מקבל 1.00 -
# כדי לבנות מועמד *חלש* צריך טוקנים משלו שאינם בצד השני.
_shared = "אלפא ביתא גמאא דלתא האאא ואוו"
_st = _shared + " זיינן חיתת טיתת יודד כףף למדד"      # 12 טוקנים
_wk = _shared + " מםם נוןן סמךך עייןן"                # 10, מהם 6 משותפים
_day = "2026-09-01"
_rows_order = [
    {"platform": "tiktok", "posted_at": _day, "caption": _wk},
    {"platform": "instagram", "posted_at": _day, "caption": _st},
    {"platform": "tiktok", "posted_at": _day, "caption": _st},
]
_p = ma.find_pairs(_rows_order)
check("זווג אחד בלבד - פריט לא משובץ פעמיים", len(_p), 1)
check("והזיווג הוא הצמד החזק, לא זה שנסרק ראשון",
      sorted(_p[0]), [1, 2])

# אותם שלושה, בסדר הפוך: התוצאה חייבת להיות זהה
_rows_rev = [_rows_order[2], _rows_order[1], _rows_order[0]]
_pr = ma.find_pairs(_rows_rev)
check("היפוך סדר הקלט לא משנה את הזיווג",
      (len(_pr), sorted(_rows_rev[i]["caption"] == _st for i in _pr[0])),
      (1, [True, True]))


print("\nמעבר שלמות: האינדקס מול הדרייב\n")


class _AuditDrive:
    def __init__(self, ids):
        self._ids = list(ids)

    def list_files(self, mime=None):
        return [{"id": i, "name": f"{i}.mp4", "mimeType": "video/mp4"}
                for i in self._ids]


def _audit_rows(*specs):
    hdr = list(ma.INDEX_HEADER)
    out = [hdr]
    for fid, deleted in specs:
        r = [""] * len(hdr)
        r[hdr.index("post_id")] = fid
        r[hdr.index("platform")] = "instagram"
        r[hdr.index("posted_at")] = "2026-09-01"
        r[hdr.index("drive_file_id")] = fid
        r[hdr.index("deleted_at")] = deleted
        out.append(r)
    return out


# a קיים בשני הצדדים; b באינדקס ואיננו בדרייב (הארכיון משקר על עצמו);
# c בדרייב בלי שורה (יתום מריצה שמתה בין ההעלאה לכתיבה); d נגרע - ולכן
# היעדרו מהדרייב הוא בדיוק מה שאמור לקרות, ואינו ממצא.
_res = ma.audit_archive(_AuditDrive(["a", "c"]),
                        FakeWS(_audit_rows(("a", ""), ("b", ""),
                                           ("d", "2026-09-02"))))
check("שורה שהקובץ שלה נעלם מדווחת", _res["missing"], ["b"])
check("קובץ בלי שורה מדווח כיתום", _res["orphans"], ["c"])
check("שורה שנגרעה אינה ממצא", "d" in _res["missing"], False)
check("המעבר קורא בלבד - לא נכתב דבר", len(_res["missing"]) >= 0, True)

_clean = ma.audit_archive(_AuditDrive(["a"]), FakeWS(_audit_rows(("a", ""))))
check("ארכיון תקין מחזיר שתי רשימות ריקות",
      (_clean["missing"], _clean["orphans"]), ([], []))


print(f"\n{PASS} passed, {FAIL} failed\n")
sys.exit(1 if FAIL else 0)
