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

print(f"\n{PASS} passed, {FAIL} failed\n")
sys.exit(1 if FAIL else 0)
