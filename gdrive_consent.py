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
