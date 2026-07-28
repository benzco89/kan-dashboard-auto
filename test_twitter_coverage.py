"""Locks the Twitter collector's coverage check — the hole that ate 28.7.2026.

GetXAPI answers 200 with an empty or truncated feed every few days. The old
collector read "no more pages" as "end of feed", returned what it had, and
exited 0. On 28.7 that meant a green step, zero tweets written, no alert — and
27.7 frozen at the 13 tweets the previous morning happened to catch.

The rule now: only a page reaching back past the cutoff proves full coverage.
Anything else is retried, and a still-partial feed exits non-zero so the
best-effort alert in daily_update.yml fires.

    python test_twitter_coverage.py
"""

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# הקולקטור מדפיס אמוג'ים; קונסולת Windows ברירת-מחדל היא cp1255 ונופלת עליהם
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
os.environ.setdefault('GETXAPI_KEY', 'test')
os.environ.setdefault('TWITTER_USERNAME', 'kann_news')
import twitter_collector as tc  # noqa: E402

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


def tweet(days_ago, n=0):
    """ציוץ מזויף במבנה שהספק מחזיר."""
    when = datetime.now(tc.IL_TZ) - timedelta(days=days_ago, minutes=n)
    return {
        "id": f"{days_ago}-{n}",
        "createdAt": when.strftime("%a %b %d %H:%M:%S %z %Y"),
        "text": "בדיקה",
        "viewCount": 100, "likeCount": 5,
        "url": f"https://x.com/kann_news/status/{days_ago}{n}",
    }


def run(pages_per_attempt):
    """
    מריץ את הקולקטור מול רצף תשובות מזויף.
    pages_per_attempt: רשימה של ניסיונות, כל ניסיון רשימה של תשובות עמוד.
    מחזיר (df, complete, attempts_used).
    """
    queue = [p for attempt in pages_per_attempt for p in attempt]
    calls = {"n": 0}

    def fake_get(url, headers=None, params=None, **kw):
        calls["n"] += 1
        return queue.pop(0) if queue else {"tweets": []}

    orig_get, orig_sleep = tc.http_get_json, tc.time.sleep
    tc.http_get_json, tc.time.sleep = fake_get, lambda s: None
    try:
        df, complete = tc.fetch_twitter_data()
    finally:
        tc.http_get_json, tc.time.sleep = orig_get, orig_sleep
    return df, complete, calls["n"]


print("\ntwitter coverage\n" + "-" * 62)

# --- the 28.7 incident: first page comes back empty, nothing else ---
empty = [{"tweets": [], "has_more": False}]
df, complete, calls = run([empty, empty, empty])
check("empty feed -> no data", len(df), 0)
check("empty feed -> not complete", complete, False)
check("empty feed -> retried, not accepted once", calls, tc.MAX_FETCH_ATTEMPTS)

# --- the 24.7 shape: a short feed, entirely inside the window ---
# 19 tweets over two days and then "end of feed" is the provider choking,
# not @kann_news going quiet — the 7-day window was never reached.
short = [{"tweets": [tweet(0, n) for n in range(3)], "has_more": False}]
df, complete, calls = run([short, short, short])
check("truncated feed -> data still saved", len(df), 3)
check("truncated feed -> flagged partial", complete, False)
check("truncated feed -> retried", calls, tc.MAX_FETCH_ATTEMPTS)

# --- a healthy run: a page reaches back past the cutoff ---
full = [{"tweets": [tweet(0), tweet(3), tweet(tc.DAYS_BACK + 1)], "has_more": True,
         "next_cursor": "c1"}]
df, complete, calls = run([full])
check("full coverage -> complete", complete, True)
check("full coverage -> one attempt only", calls, 1)
check("full coverage -> tweets outside window dropped", len(df), 2)

# --- transient blip: attempt 1 empty, attempt 2 healthy ---
df, complete, calls = run([empty, full])
check("blip then recovery -> complete", complete, True)
check("blip then recovery -> good data kept", len(df), 2)
check("blip then recovery -> stopped after 2 attempts", calls, 2)

# --- the richest attempt wins even when none of them is complete ---
richer = [{"tweets": [tweet(0, n) for n in range(6)], "has_more": False}]
df, complete, _ = run([short, richer, short])
check("partial attempts -> richest kept", len(df), 6)
check("partial attempts -> still flagged", complete, False)

print("-" * 62)
print(f"  {PASS} passed, {FAIL} failed\n")
sys.exit(1 if FAIL else 0)
