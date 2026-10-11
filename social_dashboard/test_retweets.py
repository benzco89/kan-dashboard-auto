"""Locks how the dashboard treats a retweet — someone else's tweet.

Its `views` is the retweet wrapper's own counter, ~1,100 whatever the original
did (Grovais: 1,104 here, 32,613 on X; probe run 38129405158). Ranked as Kan's
content it filled the overview's "חלשים" list — 6 of 8 slots over 30 days.
Now it stays out of every total, ranking and alert, and the Twitter page lists
it apart with the original's numbers. Rows saved before `orig_views` existed
are caught by the "RT @" prefix and show no number rather than a wrong one.

    python social_dashboard/test_retweets.py
"""

import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import aggregate  # noqa: E402

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


day = (aggregate.israel_today() - timedelta(days=2)).isoformat()


def tweet(tid, text, views, likes=5, **extra):
    r = {"tweet_id": tid, "date": day, "time": "10:00", "type": "Text", "text": text, "views": str(views),
         "likes": str(likes), "retweets": "1", "replies": "0", "quotes": "0", "bookmarks": "0",
         "permalink": f"https://x.com/kann_news/status/{tid}", "pulled_at": day + " 08:40",
         "retweet_of": "", "orig_tweet_id": "", "orig_views": "", "orig_likes": ""}
    r.update(extra)
    return r


OWN_A = tweet("1", "דובר צה\"ל: כוחות פועלים", 4000)
OWN_B = tweet("2", "שר החינוך קיש", 9000)
NEW_RT = tweet("3", "RT @daniel_grovais: תיעוד: ראש ישיבת", 1106, likes=0,
               retweet_of="daniel_grovais", orig_tweet_id="99", orig_views="32613", orig_likes="276")
OLD_RT = tweet("4", "RT @gilicohen10: 7 שאלות באוקטובר", 1024, likes=0)   # saved before the columns
rows = [OWN_A, OWN_B, NEW_RT, OLD_RT]

print("recognising a retweet")
check("own tweet", aggregate._is_retweet(OWN_A), False)
check("new row: retweet_of filled", aggregate._is_retweet(NEW_RT), True)
check("old row: only the RT @ prefix", aggregate._is_retweet(OLD_RT), True)
check("RT mentioned mid-text is not a retweet", aggregate._is_retweet(tweet("5", "הסבר: RT @x זה ריטווט", 10)), False)
check("_own_tweets keeps order, drops both kinds", [r["tweet_id"] for r in aggregate._own_tweets(rows)], ["1", "2"])
check("_own_tweets on None", aggregate._own_tweets(None), [])

print("the retweet row shown on the Twitter page")
r = aggregate._retweet_row(NEW_RT)
check("author", r["author"], "daniel_grovais")
check("text without the RT prefix", r["title"], "תיעוד: ראש ישיבת")
check("original's views, not the wrapper's 1,106", r["views"], 32613)
check("original's likes", r["likes"], 276)
r = aggregate._retweet_row(OLD_RT)
check("old row: author from the text", r["author"], "gilicohen10")
check("old row: no number, not the wrapper's and not 0", (r["views"], r["likes"]), (None, None))
r = aggregate._retweet_row(tweet("6", "RT @gilicohen10: הפרויקט המלא https://t.co/RNgT2J0KDY ב-@ReshetBet https://t.co/x", 1, likes=0))
check("links removed, so the RTL line does not flip", r["title"], "הפרויקט המלא ב-@ReshetBet")

print("build_twitter")
data = {"twitter": rows, "followers": []}
page = aggregate.build_twitter(data, 7)
check("ranked table: own tweets only", sorted(p["url"][-1] for p in page["posts"]), ["1", "2"])
check("total views: own tweets only", page["kpis"]["views"]["value"], 13000)
check("tweet count: own tweets only", page["summary"]["total"], 2)
check("retweets listed apart", sorted(x["author"] for x in page["retweets"]), ["daniel_grovais", "gilicohen10"])

print(f"\n{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
