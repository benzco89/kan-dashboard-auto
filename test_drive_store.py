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

print(f"\n{PASS} passed, {FAIL} failed\n")
sys.exit(1 if FAIL else 0)
