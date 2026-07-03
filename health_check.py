"""
Health Check Script for Kan News Social Media Analytics.
Runs daily after data collection to verify everything is working correctly.
Sends Telegram alerts if any issues are detected.

Checks per platform sheet:
  1. Freshness  - the collector actually ran today (max pulled_at/last_updated).
  2. Coverage   - the last two days have at least one row. Two days, not one:
                  a single quiet publish day (e.g. Yom Kippur) is legitimate
                  and must not page anyone.
  3. Sanity     - recent rows are not all-zero on key metrics (an all-zero
                  column = silent API breakage, e.g. a metric Meta removed).
                  Note: the collectors' backfill guard also detects this at
                  collection time and fails the collector step - this check
                  is the second net, and the primary one for day-1 breakage
                  on rows that have no previous value to backfill from.

Platforms marked optional (Twitter/GetXAPI is best-effort in daily_update.yml)
produce WARNINGS: included in the Telegram message but do not fail the job.
"""
import gspread
from datetime import datetime, timedelta
import json
import os
import pytz

from utils import send_telegram_alert

# Configuration
SPREADSHEET_ID = '1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c'
MIN_ROWS_THRESHOLD = 5  # Alert if sheet has fewer rows than this

# Per-platform check config: which columns hold the row date, the pull
# timestamp, and the metrics that must not be all-zero for recent rows.
PLATFORM_CHECKS = [
    {
        'sheet': 'נתוני יוטיוב',
        'date_col': 'published_at',
        'pulled_col': 'last_updated',
        'nonzero_cols': ['views'],
    },
    {
        'sheet': 'נתוני פייסבוק',
        'date_col': 'date',
        'pulled_col': 'pulled_at',
        'nonzero_cols': ['views', 'reach', 'likes'],
    },
    {
        'sheet': 'נתוני אינסטגרם',
        'date_col': 'date',
        'pulled_col': 'pulled_at',
        'nonzero_cols': ['views', 'reach', 'likes'],
    },
    {
        'sheet': 'נתוני טוויטר',
        'date_col': 'date',
        'pulled_col': 'pulled_at',
        'nonzero_cols': ['views'],
        'optional': True,  # GetXAPI הוא best-effort גם ב-daily_update.yml
    },
]


def _num(v):
    try:
        return float(str(v).replace(',', ''))
    except (ValueError, TypeError):
        return 0.0


def check_platform_sheet(spreadsheet, cfg, today, recent_days):
    """Run freshness/coverage/sanity checks on one platform sheet."""
    problems = []
    name = cfg['sheet']

    try:
        sheet = spreadsheet.worksheet(name)
        records = sheet.get_all_records()
    except Exception as e:
        return [f"{name}: {e}"]

    if len(records) < MIN_ROWS_THRESHOLD:
        problems.append(f"{name}: רק {len(records)} שורות (מצופה לפחות {MIN_ROWS_THRESHOLD})")
        return problems

    # 1. Freshness: the collector wrote today
    pulled_col = cfg['pulled_col']
    pull_dates = [str(r.get(pulled_col, ''))[:10] for r in records if r.get(pulled_col)]
    last_pull = max(pull_dates) if pull_dates else ''
    if last_pull < today.strftime('%Y-%m-%d'):
        problems.append(f"{name}: האיסוף לא רץ היום (משיכה אחרונה: {last_pull or 'לא ידוע'})")
        # No fresh data - the value checks below would only repeat the same problem
        return problems

    # 2. Coverage: the last two days have rows (one quiet day is legitimate)
    recent_strs = {d.strftime('%Y-%m-%d') for d in recent_days}
    recent_rows = [r for r in records if str(r.get(cfg['date_col'], ''))[:10] in recent_strs]
    if not recent_rows:
        days_txt = ' / '.join(sorted(recent_strs))
        problems.append(f"{name}: אין אף פוסט מהיומיים האחרונים ({days_txt}) - ייתכן שהאיסוף מחזיר ריק")
        return problems

    # 3. Sanity: recent rows are not all-zero on key metrics
    for col in cfg['nonzero_cols']:
        if col not in recent_rows[0]:
            continue
        if all(_num(r.get(col)) == 0 for r in recent_rows):
            problems.append(
                f"{name}: כל השורות מהיומיים האחרונים עם {col}=0 ({len(recent_rows)} שורות) - "
                f"חשד לשבירת מטריקה ב-API (כמו הסרת reach ב-v25)"
            )

    return problems


def check_data_freshness():
    """
    Check that all data sources have been updated recently.
    Returns (errors, warnings) - warnings come from optional platforms.
    """
    # Load credentials
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT')
    if not creds_json:
        return ["GCP_SERVICE_ACCOUNT environment variable not set"], []

    try:
        creds = json.loads(creds_json)
        gc = gspread.service_account_from_dict(creds)
    except Exception as e:
        return [f"Failed to authenticate with Google Sheets: {e}"], []

    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        return [f"Failed to open spreadsheet: {e}"], []

    errors = []
    warnings = []
    il_tz = pytz.timezone('Asia/Jerusalem')
    today = datetime.now(il_tz).date()
    yesterday = today - timedelta(days=1)
    recent_days = [yesterday, today - timedelta(days=2)]

    # Check 1: Followers sheet has recent data
    try:
        followers = spreadsheet.worksheet('מעקב עוקבים')
        all_values = followers.get_all_values()
        if len(all_values) < 2:
            errors.append("מעקב עוקבים: Sheet is empty!")
        else:
            last_row = all_values[-1]
            try:
                last_date = datetime.strptime(last_row[0], '%Y-%m-%d').date()
                if last_date < yesterday:
                    errors.append(f"מעקב עוקבים: עדכון אחרון {last_date} (לפני {(today - last_date).days} ימים)")
            except ValueError:
                errors.append(f"מעקב עוקבים: Invalid date format in last row: {last_row[0]}")
    except Exception as e:
        errors.append(f"מעקב עוקבים: {e}")

    # Check 2: Per-platform freshness + coverage + value sanity
    for cfg in PLATFORM_CHECKS:
        problems = check_platform_sheet(spreadsheet, cfg, today, recent_days)
        if cfg.get('optional'):
            warnings.extend(problems)
        else:
            errors.extend(problems)

    return errors, warnings


def main():
    """Run all health checks and send alerts if needed."""
    print("=" * 50)
    print("Health Check - Kan News Social Analytics")
    print("=" * 50)
    print()

    errors, warnings = check_data_freshness()

    if errors or warnings:
        print("HEALTH CHECK ISSUES:")
        print()
        for error in errors:
            print(f"  - [ERROR] {error}")
        for warning in warnings:
            print(f"  - [WARN]  {warning}")
        print()

        # Send Telegram alert
        message = "⚠️ <b>Health Check Failed!</b>\n\n" if errors else "ℹ️ <b>Health Check Warnings</b>\n\n"
        message += "\n".join(f"• {e}" for e in errors)
        if warnings:
            if errors:
                message += "\n"
            message += "\n".join(f"• (אופציונלי) {w}" for w in warnings)
        message += "\n\nCheck GitHub Actions logs for details."

        if send_telegram_alert(message, parse_mode='HTML'):
            print("Telegram alert sent successfully")
        else:
            print("Failed to send Telegram alert")

        # רק שגיאות בפלטפורמות חובה מכשילות את הבדיקה
        exit(1 if errors else 0)
    else:
        print("✅ All health checks passed!")
        print()
        print("Verified:")
        print("  - מעקב עוקבים: Recent data present")
        for cfg in PLATFORM_CHECKS:
            print(f"  - {cfg['sheet']}: fresh pull today, recent coverage, metrics non-zero")

        exit(0)


if __name__ == '__main__':
    main()
