"""Locks how the dashboard treats a retweet — someone else's tweet.

Its `views` is the retweet wrapper's own counter, ~1,100 whatever the original
did (Grovais: 1,104 here, 32,613 on X; probe run 38129405158). Ranked as Kan's
content it filled the overview's "חלשים" list — 6 of 8 slots over 30 days —
with a number nobody could match against X.
The row now stays in the lists with both numbers: `views` is what came through
Kan's retweet, `orig_views` the whole tweet. Its interactions are not Kan's
(0 likes, the original's retweet count), so they stay out of the interaction
totals and show as a dash. Rows saved before `orig_views` existed are caught by
the "RT @" prefix and show no whole-tweet number rather than a wrong one.

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

print("what a list row shows for a retweet")
r = aggregate._retweet_info(NEW_RT)
check("author", r["retweet_of"], "daniel_grovais")
check("the whole tweet's views", r["orig_views"], 32613)
check("the whole tweet's likes", r["orig_likes"], 276)
r = aggregate._retweet_info(OLD_RT)
check("old row: author from the text", r["retweet_of"], "gilicohen10")
check("old row: no whole-tweet number, not 0", (r["orig_views"], r["orig_likes"]), (None, None))
check("own tweet: nothing", aggregate._retweet_info(OWN_A), {"retweet_of": "", "orig_views": None, "orig_likes": None})
check("title drops the RT prefix", aggregate._tweet_title(NEW_RT), "תיעוד: ראש ישיבת")
check("title drops links, so the RTL line does not flip",
      aggregate._tweet_title(tweet("6", "RT @gilicohen10: הפרויקט המלא https://t.co/RNgT2J0KDY ב-@ReshetBet https://t.co/x", 1)),
      "הפרויקט המלא ב-@ReshetBet")
check("own tweet title untouched, links and all", aggregate._tweet_title(tweet("7", "ידיעה https://t.co/x", 1)), "ידיעה https://t.co/x")

print("build_twitter")
page = aggregate.build_twitter({"twitter": rows, "followers": []}, 7)
by = {p["url"][-1]: p for p in page["posts"]}
check("retweets stay in the table", sorted(by), ["1", "2", "3", "4"])
check("retweet row: views = through Kan's retweet", by["3"]["views"], 1106)
check("retweet row: whole tweet alongside", (by["3"]["retweet_of"], by["3"]["orig_views"]), ("daniel_grovais", 32613))
check("retweet row: interactions are not Kan's -> None",
      (by["3"]["likes"], by["3"]["retweets"], by["3"]["engagement"]), (None, None, None))
check("own row: interactions kept", (by["1"]["likes"], by["1"]["retweets"]), (5, 1))
check("views KPI counts what came through the retweets", page["kpis"]["views"]["value"], 4000 + 9000 + 1106 + 1024)
check("likes KPI: own tweets only", page["kpis"]["likes"]["value"], 10)
check("retweets KPI: not the originals' counts", page["kpis"]["retweets"]["value"], 2)
check("tweet count includes retweets", page["summary"]["total"], 4)

print(f"\n{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
