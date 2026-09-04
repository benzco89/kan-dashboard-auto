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
