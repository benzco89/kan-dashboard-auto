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
