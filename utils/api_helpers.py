"""
API Helper utilities for social media collectors.
Provides retry logic with exponential backoff for resilient API calls.
"""
import time
import functools


def retry_with_backoff(max_retries=3, base_delay=1, max_delay=30):
    """
    Decorator for API calls with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay in seconds (default: 1)
        max_delay: Maximum delay between retries in seconds (default: 30)

    Usage:
        @retry_with_backoff(max_retries=3)
        def fetch_data():
            return api.get_data()
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        print(f"    Attempt {attempt + 1}/{max_retries} failed: {e}")
                        print(f"    Retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        print(f"    All {max_retries} attempts failed for {func.__name__}")
            raise last_exception
        return wrapper
    return decorator


def http_get_json(url, params=None, headers=None, timeout=30, max_retries=3, base_delay=2):
    """
    GET request that always has a timeout and retries transient failures.

    Retries on network errors, timeouts, HTTP 429 and 5xx. Does NOT retry
    application-level errors (e.g. Graph API {'error': ...} with 200/4xx) -
    those are the caller's to handle. Raises on final failure so the caller
    can decide between skipping the item and keeping a previous value.
    """
    import requests

    last_exception = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code} from {url.split('?')[0]}")
            return resp.json()
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                delay = min(base_delay * (2 ** attempt), 30)
                print(f"    Request failed (attempt {attempt + 1}/{max_retries}): {e} - retrying in {delay}s")
                time.sleep(delay)
    raise last_exception


def backfill_zero_metrics(new_df, existing_df, key, cols):
    """
    Protect the sheet from transient fetch failures: where a metric in new_df
    is 0 but the same row (matched by `key`) already has a positive value in
    the sheet, keep the existing value. Cumulative metrics (views, reach,
    likes...) never legitimately drop to 0, so a 0-over-positive is always a
    failed/lagging API call. Run this BEFORE delta computation, so a backfilled
    value yields delta 0 instead of a huge negative.

    Returns new_df (modified in place) after backfilling.
    """
    import pandas as pd

    if new_df.empty or existing_df.empty or key not in existing_df.columns:
        return new_df

    existing = existing_df.copy()
    existing[key] = existing[key].astype(str)
    new_df[key] = new_df[key].astype(str)

    backfilled = 0
    for col in cols:
        if col not in new_df.columns or col not in existing.columns:
            continue
        old_map = pd.to_numeric(existing.set_index(key)[col], errors='coerce').fillna(0).to_dict()

        def _fill(row, col=col):
            nonlocal backfilled
            new_val = pd.to_numeric(pd.Series([row[col]]), errors='coerce').fillna(0).iloc[0]
            old_val = old_map.get(row[key], 0)
            if new_val == 0 and old_val > 0:
                backfilled += 1
                return old_val
            return row[col]

        new_df[col] = new_df.apply(_fill, axis=1)

    if backfilled:
        print(f"    [guard] Backfilled {backfilled} zero metric values from existing sheet data (transient API failures)")
    return new_df


def send_telegram_alert(message, token=None, chat_id=None):
    """
    Send alert message to Telegram.

    Args:
        message: The message to send
        token: Telegram bot token (or reads from TELEGRAM_TOKEN env var)
        chat_id: Telegram chat ID (or reads from TELEGRAM_CHAT_ID env var)
    """
    import os
    import requests

    token = token or os.environ.get('TELEGRAM_TOKEN')
    chat_id = chat_id or os.environ.get('TELEGRAM_CHAT_ID')

    if not token or not chat_id:
        print("    Warning: Telegram credentials not configured, skipping alert")
        return False

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        response = requests.post(url, data={'chat_id': chat_id, 'text': message})
        return response.ok
    except Exception as e:
        print(f"    Failed to send Telegram alert: {e}")
        return False
