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

    @retry_with_backoff(max_retries=max_retries, base_delay=base_delay)
    def _do():
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        if resp.status_code == 429 or resp.status_code >= 500:
            raise requests.HTTPError(f"HTTP {resp.status_code} from {url.split('?')[0]}")
        return resp.json()

    return _do()


# Fraction of matched rows that must come back 0 (where the sheet had a
# positive value) before we treat the metric as broken rather than noisy.
SUSPICIOUS_BACKFILL_RATIO = 0.8
SUSPICIOUS_BACKFILL_MIN_ROWS = 5


def backfill_zero_metrics(new_df, existing_df, key, cols):
    """
    Protect the sheet from transient fetch failures: where a metric in new_df
    is 0 but the same row (matched by `key`) already has a positive value in
    the sheet, keep the existing value. The protected metrics are counters
    (views, reach, likes...) or rates derived from them - none legitimately
    drops from positive to 0, so a 0-over-positive is always a failed/lagging
    API call. Rate columns are included on purpose: when their components are
    backfilled, keeping the matching previous rate is more consistent than
    writing a 0 computed from a failed fetch. Run BEFORE delta computation,
    so a backfilled value yields delta 0 instead of a huge negative.

    NOTE for future columns: a new metric column must be added to the
    caller's `cols` list to be protected.

    Returns (new_df, suspicious_cols). new_df is modified in place.
    suspicious_cols lists columns where >=80% of the rows that had a
    positive sheet value came back 0 - that is not per-post noise but a
    broken metric (this is exactly what Meta's v25 removal of reach looked
    like). The sheet is still protected with backfilled values, so callers
    MUST surface suspicious_cols loudly (exit non-zero) - otherwise the
    backfill hides the breakage from the all-zero health check.
    """
    import pandas as pd

    if new_df.empty or existing_df.empty or key not in existing_df.columns:
        return new_df, []

    existing = existing_df.copy()
    existing[key] = existing[key].astype(str)
    new_df[key] = new_df[key].astype(str)
    existing_indexed = existing.set_index(key)

    backfilled = 0
    suspicious_cols = []
    for col in cols:
        if col not in new_df.columns or col not in existing_indexed.columns:
            continue
        old_map = pd.to_numeric(existing_indexed[col], errors='coerce').fillna(0)
        new_num = pd.to_numeric(new_df[col], errors='coerce').fillna(0)
        old_num = new_df[key].map(old_map).fillna(0)

        mask = (new_num == 0) & (old_num > 0)
        n_mask = int(mask.sum())
        if n_mask:
            new_df.loc[mask, col] = old_num[mask]
            backfilled += n_mask

        matched_positive = int((old_num > 0).sum())
        if (matched_positive >= SUSPICIOUS_BACKFILL_MIN_ROWS
                and n_mask / matched_positive >= SUSPICIOUS_BACKFILL_RATIO):
            suspicious_cols.append(col)

    if backfilled:
        print(f"    [guard] Backfilled {backfilled} zero metric values from existing sheet data (transient API failures)")
    if suspicious_cols:
        print(f"    [guard] SUSPICIOUS: {suspicious_cols} came back 0 for most rows that had positive values - "
              f"possible API metric breakage (like the v25 reach removal). Sheet protected with previous values.")
    return new_df, suspicious_cols


def send_telegram_alert(message, token=None, chat_id=None, parse_mode=None):
    """
    Send alert message to Telegram.

    Args:
        message: The message to send
        token: Telegram bot token (or reads from TELEGRAM_TOKEN env var)
        chat_id: Telegram chat ID (or reads from TELEGRAM_CHAT_ID env var)
        parse_mode: Optional Telegram parse mode (e.g. 'HTML')
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
        data = {'chat_id': chat_id, 'text': message}
        if parse_mode:
            data['parse_mode'] = parse_mode
        response = requests.post(url, data=data, timeout=30)
        return response.ok
    except Exception as e:
        print(f"    Failed to send Telegram alert: {e}")
        return False
