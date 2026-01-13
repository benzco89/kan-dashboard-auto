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
