"""Locks the competitors page — every window on it, because every one broke.

Audit of 2026-09-27 against the live sheet: Kan's growth read 482 at 7, 14, 30
and 90 days (a weekly sum), the arena moved with the range picker, and every
"coverage gap" was one rival's own exclusive. Each block below replays one of
those against a fixture with a fixed today.

    python social_dashboard/test_competitors.py
"""

import json
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import competitors_page as C  # noqa: E402

PASS = FAIL = 0
TODAY = date(2026, 9, 27)
NOW = datetime(2026, 9, 27, 12, 0)


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


def days_between(a, b):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def snaps(user, first, step, start_value, last=TODAY):
    return [{"date": str(d), "username": user, "name": user.upper(),
             "followers": start_value + i * step, "followers_change": step,
             "eng_per_1k": 5, "pulled_at": f"{d} 08:40"}
            for i, d in enumerate(days_between(first, last))]


SNAPS = (snaps("aaa", date(2026, 7, 13), 10, 1000)
         + snaps("bbb", date(2026, 7, 13), 1, 5000)
         + snaps("ccc", date(2026, 9, 25), 5, 300))            # added two days ago
FOLLOWERS = [{"date": str(d), "ig_followers": 280000 + i * 50, "ig_followers_change": 50}
             for i, d in enumerate(days_between(date(2026, 6, 1), TODAY))]

print("\ncomparison window\n" + "-" * 62)
by = C.snapshots_by_user(SNAPS)
kan = C.kan_entries(FOLLOWERS)

w7 = C.comparison_window(by, 7)
check("7d: ends on the latest snapshot", w7["end"], TODAY)
check("7d: base is end minus 7", w7["base"], date(2026, 9, 20))
check("7d: not partial", w7["partial"], False)
check("7d: rival growth", C.growth(by["aaa"], w7)["change"], 70)
check("7d: Kan measured over the same pair", C.growth(kan, w7)["change"], 350)
check("7d: same base date for Kan and a rival",
      C.growth(kan, w7)["base_date"], C.growth(by["aaa"], w7)["base_date"])

w30 = C.comparison_window(by, 30)
check("30d: Kan growth follows the range (was stuck on the weekly figure)",
      C.growth(kan, w30)["change"], 1500)
check("30d: percent is against the base", C.growth(by["aaa"], w30)["pct"],
      round(300 / (1000 + 46 * 10) * 100, 2))

w90 = C.comparison_window(by, 90)
check("90d: history shorter than the range is partial", w90["partial"], True)
check("90d: base moves to the start of history", w90["base"], date(2026, 7, 13))
check("90d: covered days are reported", w90["covered_days"], 76)
check("90d: Kan uses the moved base too", C.growth(kan, w90)["base_date"], "2026-07-13")
check("90d: Kan change over 76 days", C.growth(kan, w90)["change"], 76 * 50)

check("a new account is not ranked on growth", C.growth(by["ccc"], w7)["status"], "new")
check("and has no change", C.growth(by["ccc"], w7)["change"], None)
check("an account without snapshots has no growth",
      C.growth([], w7)["status"], "none")

print("\narena and posting rate\n" + "-" * 62)


def post(user, d, t, likes, comments=0, caption="", pulled=None, pid=None):
    return {"post_id": pid or f"{user}-{d}-{t}-{likes}", "username": user, "date": d, "time": t,
            "type": "VIDEO", "caption": caption, "likes": likes, "comments": comments,
            "permalink": f"https://instagram.com/p/{user}{d}{t}", "pulled_at": pulled or f"{d} 23:00"}


# 16 strong posts ten days old and one weak post from yesterday: the weak one
# must still reach the arena (it used to be cut by a per-account top-15 first)
ARENA_POSTS = ([post("aaa", "2026-09-17", f"{h:02d}:00", 5000) for h in range(16)]
               + [post("bbb", "2026-09-26", "10:00", 10)]
               + [post("aaa", "2026-09-27", "07:00", 9000)])           # today: outside
KAN_IG = [{"media_id": "k1", "date": "2026-09-25", "time": "09:00", "type": "Reel",
           "caption": "כאן", "likes": 100, "comments": 5, "permalink": "https://instagram.com/p/k1"}]
NAMES = {"aaa": "AAA", "bbb": "BBB"}

a = C.arena(ARENA_POSTS, KAN_IG, NAMES, TODAY)
check("arena window is the day before yesterday and yesterday", a["dates"], ["2026-09-25", "2026-09-26"])
check("a weak fresh post still makes the arena", [p["username"] for p in a["posts"]], ["kan_news", "bbb"])
check("Kan is marked", a["posts"][0]["is_kan"], True)
check("today's posts are outside the window", any(p["date"] == "2026-09-27" for p in a["posts"]), False)

# one big account must not fill the arena: at most ARENA_PER_ACCOUNT posts each
# (live 27.9: 11 of 12 slots were N12, because the ranking is absolute)
FLOOD = [post("aaa", "2026-09-26", f"{h:02d}:00", 9000 - h) for h in range(10)] + ARENA_POSTS
fa = C.arena(FLOOD, KAN_IG, NAMES, TODAY)
check("one account gets at most ARENA_PER_ACCOUNT slots",
      sum(p["username"] == "aaa" for p in fa["posts"]), C.ARENA_PER_ACCOUNT)
check("and the others still get in", {p["username"] for p in fa["posts"]}, {"aaa", "bbb", "kan_news"})
check("the account's strongest posts are the ones kept", [p["eng"] for p in fa["posts"] if p["username"] == "aaa"],
      [9000, 8999])

FEED = [post("aaa", str(d), "10:00", 1) for d in days_between(date(2026, 9, 13), TODAY) for _ in (0, 1)]
fw7 = C.feed_window(FEED, 7, TODAY)
check("7d feed window: last seven full days", (str(fw7["first"]), str(fw7["last"]), fw7["days"]),
      ("2026-09-20", "2026-09-26", 7))
check("posts per day is an average over the window", C.posts_per_day(FEED, fw7), 2.0)
fw30 = C.feed_window(FEED, 30, TODAY)
check("30d is clipped to what the feed keeps", fw30["days"], 14)
check("no feed, no rate", C.posts_per_day(FEED, C.feed_window([], 7, TODAY)), None)

print("\ncoverage gaps\n" + "-" * 62)

MATURE = ([post("aaa", "2026-09-20", "10:00", 200, pulled="2026-09-24 08:40", pid=f"am{i}") for i in range(10)]
          + [post("bbb", "2026-09-20", "10:00", 100, pulled="2026-09-24 08:40", pid=f"bm{i}") for i in range(10)])
YOUNG = [post("aaa", "2026-09-27", "06:00", 10, pulled="2026-09-27 08:40", pid="ay")]
STORY = "הפגנה גדולה בכיכר הבימה נגד הממשלה אלפי מפגינים הגיעו"
EXCL = "ראיון בלעדי הזמרת המפורסמת מדברת על הקריירה והמשפחה שלה"
QUAKE = "רעידת אדמה חזקה הורגשה בצפון הארץ בלילה ללא נפגעים"
GAP_POSTS = MATURE + YOUNG + [
    post("aaa", "2026-09-26", "09:00", 2000, caption=STORY, pid="x1"),
    post("bbb", "2026-09-26", "11:00", 900, caption=STORY + " היום", pid="x2"),
    post("aaa", "2026-09-26", "13:00", 1500, caption=EXCL, pid="y1"),
    post("aaa", "2026-09-26", "18:00", 1200, caption=EXCL + " בערב", pid="y2"),
    post("aaa", "2026-09-26", "20:00", 3000, caption=QUAKE, pid="z1"),
    post("aaa", "2026-09-23", "10:00", 5000, caption="סיפור ישן מאוד שכבר לא בחלון הזמן", pid="old"),
    post("ddd", "2026-09-26", "10:00", 400, caption="חשבון בלי פוסטים בשלים מפרסם כתבה על מזג האוויר", pid="d1"),
]
GAP_DATA = {"competitor_posts": GAP_POSTS,
            "instagram": [{"date": "2026-09-26", "caption": "רעידת אדמה חזקה הורגשה בצפון הארץ הלילה תושבים דיווחו"}],
            "facebook": [], "youtube": [], "twitter": [], "tiktok": []}

med = C.account_medians(GAP_POSTS)
check("median comes from mature posts only", med["aaa"], 200)
check("an account with no mature post has no median", "ddd" in med, False)

g = C.coverage_gaps(GAP_DATA, NOW)
check("status ok when every source loaded", g["status"], "ok")
check("a story at two rivals is 'missed'", [x["n_outlets"] for x in g["missed"]], [2])
check("its explanation is against the lead's own median", g["missed"][0]["lead"]["ratio"], 10.0)
excl = {x["caption"][:10]: x for x in g["exclusive"]}
check("one rival twice is still one outlet", excl[EXCL[:10]]["n_outlets"], 1)
check("and keeps both links", len(excl[EXCL[:10]]["posts"]), 2)
check("a story Kan posted is not a gap",
      any(QUAKE[:10] in x["caption"] for x in g["missed"] + g["exclusive"]), False)
check("posts older than 72h are out",
      any("ישן" in x["caption"] for x in g["missed"] + g["exclusive"]), False)
zero = [x for x in g["exclusive"] if x["lead"]["username"] == "ddd"][0]
check("no median: no ratio", zero["lead"]["ratio"], None)
check("no median: the floor is the threshold", zero["lead"]["threshold"], 300)

u = C.coverage_gaps(GAP_DATA, NOW, {"instagram": "unavailable"})
check("a source that did not load: unavailable", u["status"], "unavailable")
check("and names it", u["failed"], ["instagram"])
check("and claims nothing", (u["missed"], u["exclusive"]), ([], []))
s = C.coverage_gaps(GAP_DATA, NOW, {"facebook": "stale"})
check("a stale source still shows gaps, flagged", (s["status"], len(s["missed"])), ("stale", 1))

print("\nbuild\n" + "-" * 62)
DATA = dict(GAP_DATA, competitors=SNAPS, followers=FOLLOWERS,
            competitor_posts=GAP_POSTS + ARENA_POSTS, instagram=KAN_IG + GAP_DATA["instagram"])
b7 = C.build(DATA, 7, today=TODAY, now=NOW)
b90 = C.build(DATA, 90, today=TODAY, now=NOW)
check("the arena does not move with the range", b7["arena"], b90["arena"])
check("the gaps do not move with the range", b7["gaps"], b90["gaps"])
kan7 = [c for c in b7["competitors"] if c["is_kan"]][0]
check("Kan's row carries the range growth", kan7["growth"]["change"], 350)
check("Kan's rank counts every account", (b7["summary"]["kan_rank"], b7["summary"]["ranked"]), (1, 4))
check("the partial flag reaches the payload", b90["window"]["partial"], True)
check("dates are strings in the payload", b90["window"]["base"], "2026-07-13")
check("freshness counts today's snapshots", (b7["freshness"]["updated"], b7["freshness"]["known"]), (3, 3))
check("a plain dict has no source status", b7["sources"], {})

print("\nsource status read after the lazy tabs are touched\n" + "-" * 62)


class OrderStub(dict):
    """Mirrors gsheets.SheetData: a tab's failure only shows up in
    source_status() once something actually did a get() for it (that's what
    triggers the lazy read). build() used to read source_status() before
    touching the GAP_SOURCES tabs, so an unavailable tab it hadn't looked at
    yet was invisible."""

    def __init__(self, base):
        super().__init__(base)
        self.fetched = set()

    def get(self, key, default=None):
        self.fetched.add(key)
        return super().get(key, default)

    def source_status(self):
        return {k: ("unavailable" if k == "tiktok" and k in self.fetched else "ok")
                for k in C.GAP_SOURCES}


stub = OrderStub(DATA)
b_stub = C.build(stub, 7, today=TODAY, now=NOW)
check("build() touches every GAP_SOURCES key before reading source_status",
      b_stub["gaps"]["status"], "unavailable")

stale_snaps = SNAPS + [{"date": "2026-09-26", "username": "eee", "name": "E", "followers": 10}]
f = C.freshness(C.snapshots_by_user(stale_snaps))
check("an account that missed today's run is named", f["missing"], ["eee"])

print("\nintraday log\n" + "-" * 62)
items, checked = C.gap_clusters(GAP_DATA, NOW)
check("gap_clusters keeps every story, uncut", sorted(i["kind"] for i in items),
      ["exclusive", "exclusive", "missed"])
check("each post carries its id", items[0]["posts"][0]["post_id"], "x1")
check("the closest Kan post is measured",
      C._best_overlap(frozenset("אבגדה"), [frozenset("אבזח")]), {"shared": 2, "containment": 0.5})

rows = C.candidate_rows(items, "2026-09-27 11:00", "ig:live,fb:live")
check("one run marker plus one row per story", ([r["kind"] for r in rows][:1], len(rows)), (["run"], 4))
as_read = [{k: str(v) for k, v in r.items()} for r in rows]     # the sheet hands back strings
back = C.gaps_from_log(as_read, TODAY)
live = C.coverage_gaps(GAP_DATA, NOW)
shape = lambda g: {k: g[k] for k in ("caption", "date", "time", "n_outlets", "total_eng", "lead", "posts")}
check("the log reads back as what the page would have computed",
      [shape(g) for g in back["missed"] + back["exclusive"]],
      [shape(g) for g in live["missed"] + live["exclusive"]])
check("the log is marked intraday, with its run time", (back["source"], back["run_at"]),
      ("intraday", "2026-09-27 11:00"))
empty = [{k: str(v) for k, v in r.items()} for r in C.candidate_rows([], "2026-09-27 14:00", "x")]
check("a run with no stories still counts as a run", C.gaps_from_log(empty, TODAY)["missed"], [])
check("yesterday's run is not today's", C.gaps_from_log(as_read, TODAY + timedelta(days=1)), None)
later = C.candidate_rows(items[:1], "2026-09-27 14:00", "x")
check("the latest run of the day wins", len(C.gaps_from_log(rows + later, TODAY)["exclusive"]), 0)

b_log = C.build(dict(DATA, gap_candidates=rows), 7, today=TODAY, now=NOW)
check("the page serves the intraday log when there is one", b_log["gaps"]["source"], "intraday")
check("and the morning computation otherwise", b7["gaps"]["source"], "morning")
check("freshness says when the posts were last pulled", b7["freshness"]["posts_pulled_at"], "2026-09-27 23:00")

print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
