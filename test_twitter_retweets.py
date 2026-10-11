"""Locks how the Twitter collector stores a retweet.

GetXAPI returns a retweet as a wrapper with its own view counter (~1,100, 0
likes) and the original inside `retweeted_tweet`. The sheet used to keep only
the wrapper, so a retweet of a 32,613-view tweet showed 1,104 on the dashboard
(probe run 38129405158). The wrapper stays in `views`; the original's numbers
go in four columns appended at the END of the sheet - the rows are rewritten
with a header, but a column inserted mid-header still renames every reader's
position (the 2026-07-26 incident in CLAUDE.md).

    python test_twitter_retweets.py
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
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


# the shape run 38129405158 printed for the Grovais retweet, trimmed
RETWEET = {
    "id": "2106754913342550452", "createdAt": "Sun Oct 04 14:34:47 +0000 2026",
    "text": "RT @daniel_grovais: תיעוד: ראש ישיבת המיינסטרים",
    "viewCount": 1106, "likeCount": 0, "retweetCount": 39, "replyCount": 0, "quoteCount": 0,
    "url": "https://x.com/kann_news/status/2106754913342550452",
    "retweeted_tweet": {
        "id": "2106751324482130234", "text": "תיעוד: ראש ישיבת המיינסטרים",
        "viewCount": 32613, "likeCount": 276, "retweetCount": 39,
        "author": {"userName": "daniel_grovais", "followers": 2220},
    },
}
OWN = {"id": "2109217003961471447", "text": "שר החינוך קיש", "viewCount": 836, "likeCount": 1,
       "retweeted_tweet": None}

print("retweet_fields")
f = tc.retweet_fields(RETWEET)
check("author of the original", f["retweet_of"], "daniel_grovais")
check("original's id", f["orig_tweet_id"], "2106751324482130234")
check("original's views, not the wrapper's", f["orig_views"], 32613)
check("original's likes", f["orig_likes"], 276)
check("own tweet: retweeted_tweet null -> empty", tc.retweet_fields(OWN), dict.fromkeys(tc.RETWEET_COLUMNS, ""))
check("own tweet: key missing -> empty", tc.retweet_fields({"id": "1"}), dict.fromkeys(tc.RETWEET_COLUMNS, ""))
check("empty dict -> empty", tc.retweet_fields({"retweeted_tweet": {}}), dict.fromkeys(tc.RETWEET_COLUMNS, ""))


# ---- save_to_sheets against a fake sheet ----

OLD_HEADER = ["tweet_id", "date", "time", "type", "text", "views", "likes", "retweets", "replies",
              "quotes", "bookmarks", "total_engagement", "engagement_rate", "permalink", "pulled_at",
              "views_delta"]


class FakeSheet:
    def __init__(self, records):
        self.records, self.written = records, None

    def get_all_records(self):
        return self.records

    def clear(self):
        pass

    def update(self, values):
        self.written = values


def run_save(records, new_rows):
    sheet = FakeSheet(records)

    class Book:
        def worksheet(self, _):
            return sheet

    class Client:
        def open_by_key(self, _):
            return Book()

    tc.gspread.authorize = lambda creds: Client()
    tc.Credentials.from_service_account_info = staticmethod(lambda info, scopes: None)
    os.environ["GCP_SERVICE_ACCOUNT"] = json.dumps({})
    tc.save_to_sheets(tc.pd.DataFrame(new_rows))
    header, *rows = sheet.written
    return header, [dict(zip(header, r)) for r in rows]


def row(tid, date, views, t=None):
    base = {"tweet_id": tid, "date": date, "time": "10:00", "type": "Text", "text": "x",
            "views": views, "likes": 1, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0,
            "total_engagement": 1, "engagement_rate": 0.1, "permalink": "", "pulled_at": "2026-10-11 08:46"}
    base.update(tc.retweet_fields(t or {}))
    return base


# an existing sheet in the old 16-column shape; gspread hands ids back as int
old = [dict(zip(OLD_HEADER, [2106754913342550452, "2026-10-04", "16:34", "Text", "RT @daniel_grovais: x",
                             1104, 0, 39, 0, 0, 0, 39, 3.53, "", "2026-10-10 08:46", 2])),
       dict(zip(OLD_HEADER, [2100000000000000001, "2026-09-01", "09:00", "Text", "old own tweet",
                             5000, 10, 1, 0, 0, 0, 11, 0.22, "", "2026-09-07 08:40", 0]))]
new = [row("2106754913342550452", "2026-10-04", 1106, RETWEET), row("2109217003961471447", "2026-10-11", 836, OWN)]

print("save_to_sheets: existing sheet")
header, rows = run_save(old, new)
check("old columns keep their positions", header[:len(OLD_HEADER)], OLD_HEADER)
check("retweet columns appended at the end", header[len(OLD_HEADER):], tc.RETWEET_COLUMNS)
by_id = {str(r["tweet_id"]): r for r in rows}
rt = by_id["2106754913342550452"]
check("retweet: wrapper views stay in views", rt["views"], 1106)
check("retweet: original views stored", rt["orig_views"], 32613)
check("retweet: views_delta still on the wrapper", rt["views_delta"], 2)
check("retweet: original id kept exact, as text", rt["orig_tweet_id"], "2106751324482130234")
check("own tweet: retweet columns empty", (by_id["2109217003961471447"]["retweet_of"],
                                           by_id["2109217003961471447"]["orig_views"]), ("", ""))
check("row outside the window: blank retweet columns, not 0",
      (by_id["2100000000000000001"]["retweet_of"], by_id["2100000000000000001"]["orig_views"]), ("", ""))

print("save_to_sheets: second run, sheet already has the new columns")
records = [dict(r) for r in rows]
for r in records:  # gspread numericises the id on the way back in
    if r["orig_tweet_id"]:
        r["orig_tweet_id"] = int(r["orig_tweet_id"])
header2, rows2 = run_save(records, [row("2106754913342550452", "2026-10-04", 1110, RETWEET)])
check("header unchanged on the second run", header2, header)
rt2 = {str(r["tweet_id"]): r for r in rows2}["2106754913342550452"]
check("original id still exact after a round trip", rt2["orig_tweet_id"], "2106751324482130234")

print("save_to_sheets: empty sheet")
header3, _ = run_save([], new)
check("fresh sheet: views_delta before the retweet columns",
      header3[-5:], ["views_delta"] + tc.RETWEET_COLUMNS)

print(f"\n{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
