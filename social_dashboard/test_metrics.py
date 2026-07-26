"""Locks the interaction definitions in metrics.py.

The rows are REAL rows copied out of the sheets (2026-07-26), because the whole
point of the module is that the sheet's own engagement_rate column cannot be
trusted — a synthetic row would have hidden exactly that.

    python social_dashboard/test_metrics.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import metrics  # noqa: E402

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


# A real Facebook post. Note engagement_rate=11.83 on the sheet, of which the
# overwhelming majority is `clicks` — the column this module refuses to read.
FB = dict(reach=66388, clicks=7520, views=77004, likes=312, love=14, haha=0,
          wow=3, sad=2, angry=0, comments=14, shares=10,
          total_engagement=7856, engagement_rate=11.83)

# A real Instagram reel.
IG = dict(likes=4004, comments=143, views=294908, reach=153425, saved=282,
          shares=393, total_interactions=4900, engagement_rate=3.14)

# A real tweet. X calls them replies/retweets/quotes, and bookmarks are its save.
X = dict(views=8514, likes=36, retweets=3, replies=23, quotes=0, bookmarks=1,
         total_engagement=62, engagement_rate=0.73)

YT = dict(views=162590, likes=2703, comments=86)

print("\nfacebook — reactions are ONE column, clicks never count")
# 312 is reactions.summary.total_count. love=14 / wow=3 / sad=2 are a breakdown
# OF that number, not extra reactions beside it, so summing all six would report
# 331 and count a quarter of Facebook's reactions twice.
check("reactions are the `likes` column alone", metrics.count('facebook', FB, 'likes'), 312)
check("the breakdown is never added on top", metrics.count('facebook', FB, 'likes') < 331, True)
check("comments", metrics.count('facebook', FB, 'comments'), 14)
check("shares", metrics.count('facebook', FB, 'shares'), 10)
check("total ignores the 7,520 clicks", metrics.total('facebook', FB), 336)
check("rate over views, not the sheet's 11.83%", metrics.rate('facebook', FB), 0.44)
check("facebook has no saves column", metrics.columns_for('facebook', 'saves'), ())

print("\ninstagram — saves are collected but stay out of the rate")
check("likes", metrics.count('instagram', IG, 'likes'), 4004)
check("saves are reachable", metrics.count('instagram', IG, 'saves'), 282)
check("total excludes the 282 saves", metrics.total('instagram', IG), 4004 + 143 + 393)
check("rate over views (sheet says 3.14 over reach)", metrics.rate('instagram', IG), 1.54)

print("\nx — shares is retweets PLUS quotes, comments is replies")
check("retweets+quotes", metrics.count('x', X, 'shares'), 3)
check("replies as comments", metrics.count('x', X, 'comments'), 23)
check("bookmarks excluded", metrics.total('x', X), 36 + 23 + 3)
check("'twitter' resolves to x", metrics.resolve('twitter'), 'x')

print("\nyoutube — no share count exists, so no column is offered")
check("shares resolve to nothing", metrics.count('youtube', YT, 'shares'), 0)
check("two display columns only", len(metrics.display_columns('youtube')), 2)
check("everyone else has three", len(metrics.display_columns('tiktok')), 3)

print("\nedges")
check("missing views -> 0, not a crash", metrics.rate('facebook', dict(likes=5)), 0.0)
check("string cells from the sheet", metrics.count('instagram', {'likes': '1,204'}, 'likes'), 1204)
check("empty cell", metrics.count('instagram', {'likes': ''}, 'likes'), 0)
check("unknown platform still renders", len(metrics.display_columns('mastodon')), 3)

print(f"\n{PASS}/{PASS + FAIL} passed\n")
sys.exit(1 if FAIL else 0)
