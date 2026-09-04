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
