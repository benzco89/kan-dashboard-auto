"""
Health Check Script for Kan News Social Media Analytics.
Runs daily after data collection to verify everything is working correctly.
Sends Telegram alerts if any issues are detected.
"""
import gspread
import requests
from datetime import datetime, timedelta
import json
import os
import pytz

# Configuration
SPREADSHEET_ID = '1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c'
SHEETS_TO_CHECK = ['נתוני יוטיוב', 'נתוני פייסבוק', 'נתוני אינסטגרם']
MIN_ROWS_THRESHOLD = 5  # Alert if sheet has fewer rows than this


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
        })
        return response.ok
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")
        return False


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

    # Check 2: Platform sheets have data
    for sheet_name in SHEETS_TO_CHECK:
        try:
            sheet = spreadsheet.worksheet(sheet_name)
            data = sheet.get_all_values()
            row_count = len(data)

            if row_count < 2:
                errors.append(f"{sheet_name}: Sheet is empty!")
            elif row_count < MIN_ROWS_THRESHOLD:
                errors.append(f"{sheet_name}: Only {row_count} rows (expected at least {MIN_ROWS_THRESHOLD})")

        except Exception as e:
            errors.append(f"{sheet_name}: {e}")

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
        for sheet_name in SHEETS_TO_CHECK:
            print(f"  - {sheet_name}: Data present")

        exit(0)


if __name__ == '__main__':
    main()
