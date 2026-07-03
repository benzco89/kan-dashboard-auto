"""
Health Check Script for Kan News Social Media Analytics.
Runs daily after data collection to verify everything is working correctly.
Sends Telegram alerts if any issues are detected.

Checks per platform sheet:
  1. Freshness  - the collector actually ran today (max pulled_at/last_updated).
  2. Coverage   - yesterday has at least one row (Kan publishes daily).
  3. Sanity     - yesterday's rows are not all-zero on key metrics
                  (an all-zero column = silent API breakage, e.g. a metric
                  Meta removed - this is exactly how the v25 reach outage
                  looked and the old row-count check missed it).
"""
import gspread
import requests
from datetime import datetime, timedelta
import json
import os
import pytz

# Configuration
SPREADSHEET_ID = '1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c'
MIN_ROWS_THRESHOLD = 5  # Alert if sheet has fewer rows than this

# Per-platform check config: which columns hold the row date, the pull
# timestamp, and the metrics that must not be all-zero for yesterday's rows.
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
    },
]


def send_telegram_alert(message):
    """Send alert message to Telegram."""
    token = os.environ.get('TELEGRAM_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')

    if not token or not chat_id:
        print("Warning: Telegram credentials not configured")
        return False

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        response = requests.post(url, data={
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }, timeout=30)
        return response.ok
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")
        return False


def _num(v):
    try:
        return float(str(v).replace(',', ''))
    except (ValueError, TypeError):
        return 0.0


def check_platform_sheet(spreadsheet, cfg, today, yesterday):
    """Run freshness/coverage/sanity checks on one platform sheet."""
    errors = []
    name = cfg['sheet']

    try:
        sheet = spreadsheet.worksheet(name)
        records = sheet.get_all_records()
    except Exception as e:
        return [f"{name}: {e}"]

    if len(records) < MIN_ROWS_THRESHOLD:
        errors.append(f"{name}: רק {len(records)} שורות (מצופה לפחות {MIN_ROWS_THRESHOLD})")
        return errors

    # 1. Freshness: the collector wrote today
    pulled_col = cfg['pulled_col']
    pull_dates = [str(r.get(pulled_col, ''))[:10] for r in records if r.get(pulled_col)]
    last_pull = max(pull_dates) if pull_dates else ''
    if last_pull < today.strftime('%Y-%m-%d'):
        errors.append(f"{name}: האיסוף לא רץ היום (משיכה אחרונה: {last_pull or 'לא ידוע'})")
        # No fresh data - the value checks below would only repeat the same problem
        return errors

    # 2. Coverage: yesterday has rows
    y_str = yesterday.strftime('%Y-%m-%d')
    y_rows = [r for r in records if str(r.get(cfg['date_col'], ''))[:10] == y_str]
    if not y_rows:
        errors.append(f"{name}: אין אף פוסט מאתמול ({y_str}) - ייתכן שהאיסוף החזיר ריק")
        return errors

    # 3. Sanity: yesterday's rows are not all-zero on key metrics
    for col in cfg['nonzero_cols']:
        if col not in y_rows[0]:
            continue
        if all(_num(r.get(col)) == 0 for r in y_rows):
            errors.append(
                f"{name}: כל שורות אתמול עם {col}=0 ({len(y_rows)} שורות) - "
                f"חשד לשבירת מטריקה ב-API (כמו הסרת reach ב-v25)"
            )

    return errors


def check_data_freshness():
    """
    Check that all data sources have been updated recently.
    Returns a list of error messages (empty if all checks pass).
    """
    # Load credentials
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT')
    if not creds_json:
        return ["GCP_SERVICE_ACCOUNT environment variable not set"]

    try:
        creds = json.loads(creds_json)
        gc = gspread.service_account_from_dict(creds)
    except Exception as e:
        return [f"Failed to authenticate with Google Sheets: {e}"]

    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        return [f"Failed to open spreadsheet: {e}"]

    errors = []
    il_tz = pytz.timezone('Asia/Jerusalem')
    today = datetime.now(il_tz).date()
    yesterday = today - timedelta(days=1)

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
        errors.extend(check_platform_sheet(spreadsheet, cfg, today, yesterday))

    return errors


def main():
    """Run all health checks and send alerts if needed."""
    print("=" * 50)
    print("Health Check - Kan News Social Analytics")
    print("=" * 50)
    print()

    errors = check_data_freshness()

    if errors:
        print("HEALTH CHECK FAILED!")
        print()
        for error in errors:
            print(f"  - {error}")
        print()

        # Send Telegram alert
        message = "⚠️ <b>Health Check Failed!</b>\n\n"
        message += "\n".join(f"• {e}" for e in errors)
        message += "\n\nCheck GitHub Actions logs for details."

        if send_telegram_alert(message):
            print("Telegram alert sent successfully")
        else:
            print("Failed to send Telegram alert")

        exit(1)
    else:
        print("✅ All health checks passed!")
        print()
        print("Verified:")
        print("  - מעקב עוקבים: Recent data present")
        for cfg in PLATFORM_CHECKS:
            print(f"  - {cfg['sheet']}: fresh pull today, yesterday covered, metrics non-zero")

        exit(0)


if __name__ == '__main__':
    main()
