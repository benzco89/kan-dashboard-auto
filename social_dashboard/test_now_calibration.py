"""Locks the calibration of "now at rivals" (analyze_now_thresholds.py) offline.

The number that decides NOW_CALIBRATED must not be optimistic: truth is the
count at a fixed age (not whatever the last pull was), a post is not ranked
against itself, it fires at its first early crossing (what the page would have
shown then, not its best moment), and accounts too thin to rank are named.

    venv/Scripts/python.exe test_now_calibration.py
"""

import contextlib
import io
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analyze_now_thresholds as N  # noqa: E402
import competitors_page as C  # noqa: E402

PASS = FAIL = 0
sys.stdout.reconfigure(encoding="utf-8")


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


def row(pid, user, posted, age, likes):
    return {"post_id": pid, "username": user,
            "pulled_at": (posted + timedelta(hours=age)).strftime("%Y-%m-%d %H:%M"),
            "age_h": str(age), "likes": str(likes), "comments": "0"}


T0 = datetime(2026, 9, 20, 6)
ROWS = []
# eleven ordinary posts of aaa: 100 at 3h, 200 at 10h, and 1000..1100 at 33h
for i in range(11):
    posted = T0 + timedelta(hours=7 * i)
    ROWS += [row(f"p{i}", "aaa", posted, 3, 100), row(f"p{i}", "aaa", posted, 10, 200),
             row(f"p{i}", "aaa", posted, 33, 1000 + 10 * i)]
# one hot post: 2.1x at 3h, 5x at 10h; 5000 at 33h, 9000 at its last pull (48h)
HOT = T0 + timedelta(hours=100)
ROWS += [row("hot", "aaa", HOT, 3, 210), row("hot", "aaa", HOT, 10, 1000),
         row("hot", "aaa", HOT, 33, 5000), row("hot", "aaa", HOT, 48, 9000)]
# a thin account: three posts with a 33h count
for i in range(3):
    ROWS += [row(f"b{i}", "bbb", T0 + timedelta(hours=5 * i), 33, 50)]
# Kan is never calibrated against
ROWS += [row("k0", "kan_news", T0, 33, 700)]

HIST = C.history_by_post(ROWS)
BY = C.posts_by_account(HIST)

print("truth at a fixed age\n" + "-" * 62)
obs = [("a", 5.0, 1, "u"), ("b", 30.0, 2, "u"), ("c", 34.0, 3, "u"), ("d", 48.0, 4, "u")]
check("truth is the observation closest to 33h, not the last one", N.truth_obs(obs)[1], 34.0)
check("no observation within ±4h of 33h: no truth",
      N.truth_obs([("a", 5.0, 1, "u"), ("d", 48.0, 4, "u")]), None)
truths = N.account_truths(BY["aaa"])
check("the hot post's truth is its 33h count, not the 48h one", truths["hot"], 5000)

print("\npercentile\n" + "-" * 62)
check("a post is ranked among the OTHER posts (self included would be 11.5/12)",
      N.percentile(truths, "hot"), 1.0)
check("the lowest ordinary post sits at 0 among the others", N.percentile(truths, "p0"), 0.0)

print("\nfirst firing\n" + "-" * 62)
check("at 2.0 it fires at its first early crossing (3h, 2.1x)",
      N.first_fire(HIST["hot"], BY, "aaa", "hot", 2.0), 3.0)
check("at 3.0 the 3h pull did not cross; it first crosses at 10h",
      N.first_fire(HIST["hot"], BY, "aaa", "hot", 3.0), 10.0)
check("an ordinary post never fires", N.first_fire(HIST["p3"], BY, "aaa", "p3", 1.5), None)
check("the age buckets", [N.bucket(a) for a in (3.0, 4.0, 10.0, 20.0, 24.0)],
      ["<4h", "4-8h", "8-16h", "16-24h", None])

print("\ncalibrate()\n" + "-" * 62)
res = N.calibrate(HIST)
check("an account below MIN_FINALS is reported as excluded, with its count", res["excluded"], {"bbb": 3})
check("and Kan is not an account to calibrate", "kan_news" in res["included"], False)
check("the included account with its truth count", res["included"], {"aaa": 12})
check("posts scored", res["posts_scored"], 12)
t2 = res["thresholds"][2.0]
check("at 2.0: one fired, landing at the top", (t2["overall"]["fired"], t2["overall"]["median_pct"],
                                                  t2["overall"]["share_p90"]), (1, 1.0, 1.0))
check("at 2.0 it counts under <4h", (t2["by_age"]["<4h"]["fired"], t2["by_age"]["8-16h"]["fired"]), (1, 0))
t3 = res["thresholds"][3.0]
check("at 3.0 it counts under 8-16h", (t3["by_age"]["<4h"]["fired"], t3["by_age"]["8-16h"]["fired"]), (0, 1))
check("baselines per age bucket: with / without / median",
      (res["baselines"]["<4h"], res["baselines"]["8-16h"]["with"], res["baselines"]["8-16h"]["median_base"]),
      ({"with": 12, "without": 0, "median_base": 100}, 12, 200))

buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    N.report(res)
check("the report prints the excluded account and the base rate",
      ("bbb" in buf.getvalue(), "base rate 10%" in buf.getvalue()), (True, True))

print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
