"""
Calibration Check - בדיקה חודשית שהספים המכוילים לא נשחקו.

כל הספים במערכת (רצפות תגובות לניתוח, רצפת "ממשיכים לצבור" בדוח) כוילו
פעם אחת מול צילום מצב של החשבון. נפחי החשבון זזים - הבדיקה הזאת מחשבת
מחדש את ההתפלגויות ומתריעה בטלגרם רק אם יש סחיפה של 25%+ (עיקרון
signal-over-filler: אין סחיפה = שקט מלא).

רץ ב-1 בכל חודש כחלק מהפייפליין היומי (מדולג בשאר הימים).

Env: GCP_SERVICE_ACCOUNT, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID.
     DRY_RUN=1 - הדפסה בלבד, בלי טלגרם.
"""

import os
import sys
import json
from datetime import datetime, timedelta

import gspread
import pytz
from google.oauth2.service_account import Credentials

from utils import send_telegram_alert
from comment_analyzer import PLATFORMS  # רצפות התגובות הנוכחיות

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
IL_TZ = pytz.timezone('Asia/Jerusalem')
DRY_RUN = os.environ.get('DRY_RUN', '') not in ('', '0', 'false')

# איזה אחוזון כל רצפה מייצגת (כפי שכויל במקור, 2026-07-13)
FLOOR_PERCENTILE = {'instagram': 80, 'facebook': 75, 'youtube': 90}
DRIFT = 0.25                  # מעל זה - מתריעים
OLD_VIDEO_FLOOR = 5000        # telegram_reporter: "ממשיכים לצבור" (5K+/יום)
OLD_VIDEO_TARGET = (1, 5)     # יעד: 1-5 סרטונים מוזכרים ביום בממוצע


def _num(v):
    try:
        return float(str(v).replace(',', '').strip() or 0)
    except (ValueError, TypeError):
        return 0.0


def _pct(xs, p):
    xs = sorted(x for x in xs if x > 0)
    if not xs:
        return 0.0
    k = (len(xs) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] * (1 - (k - lo)) + xs[hi] * (k - lo)


def main():
    print(f"\n🎛 Calibration Check - {datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M')}\n")

    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT') or os.environ.get('GOOGLE_CREDENTIALS')
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=[
        'https://www.googleapis.com/auth/spreadsheets.readonly'])
    sh = gspread.authorize(creds).open_by_key(SPREADSHEET_ID)

    cutoff30 = (datetime.now(IL_TZ) - timedelta(days=30)).strftime('%Y-%m-%d')
    findings = []

    # 1. רצפות התגובות של comment_analyzer
    for plat in PLATFORMS:
        rows = sh.worksheet(plat['sheet']).get_all_records()
        cm = [_num(r.get('comments')) for r in rows
              if str(r.get(plat['date_col'], ''))[:10] >= cutoff30]
        target = _pct(cm, FLOOR_PERCENTILE[plat['key']])
        suggested = max(50, round(target / 50) * 50)
        current = plat['floor']
        print(f"  {plat['key']:10} floor={current} · p{FLOOR_PERCENTILE[plat['key']]}(30d)={target:,.0f} · suggested={suggested}")
        if current and abs(suggested - current) / current > DRIFT:
            findings.append(
                f"• רצפת תגובות {plat['label']}: {current} → מוצע {suggested} "
                f"(p{FLOOR_PERCENTILE[plat['key']]} של 30 יום = {target:,.0f}). "
                f"לעדכן גם את רצפת ה\"חם\" ברחרחן (פי-2).")

    # 2. רצפת "סרטונים ישנים שממשיכים לצבור" בדוח היומי.
    # "ישן" כמו אצל telegram_reporter: פורסם לפני אתמול ורוענן בריצה האחרונה.
    yt = sh.worksheet('נתוני יוטיוב').get_all_records()
    yesterday = (datetime.now(IL_TZ) - timedelta(days=1)).strftime('%Y-%m-%d')
    fresh_old = [_num(r.get('views_delta')) for r in yt
                 if str(r.get('last_updated', ''))[:10] >= yesterday
                 and str(r.get('published_at', '')) < yesterday
                 and _num(r.get('views_delta')) > 0]
    # כמה עוברים את הרצפה "ביום" (מקורב: ההתפלגות של הריצה האחרונה)
    qualifying = sum(1 for d in fresh_old if d >= OLD_VIDEO_FLOOR)
    print(f"  old-videos floor={OLD_VIDEO_FLOOR:,} · qualifying now={qualifying}")
    if qualifying > OLD_VIDEO_TARGET[1] * 3:
        findings.append(
            f"• רצפת \"ממשיכים לצבור\" ({OLD_VIDEO_FLOOR:,}): {qualifying} סרטונים עוברים אותה - "
            f"רועש מדי, שקול להעלות (p90 של הדלתות: {_pct(fresh_old, 90):,.0f}).")

    if not findings:
        print("\n✔ אין סחיפה - כל הספים בתחום. לא נשלחה הודעה.")
        return

    message = "🎛 בדיקת כיול חודשית - נדרש כוונון:\n\n" + "\n".join(findings) + \
              "\n\nלכיול מלא של ספי ההתראות: analyze_thresholds.py"
    print("\n" + message)
    if DRY_RUN:
        print("\n🧪 DRY RUN - not sending")
        return
    send_telegram_alert(message)
    print("\n📨 Sent to Telegram")


if __name__ == "__main__":
    main()
