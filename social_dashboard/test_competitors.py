"""Locks the competitors page — every window on it, because every one broke.

Audit of 2026-09-27 against the live sheet: Kan's growth read 482 at 7, 14, 30
and 90 days (a weekly sum), the arena moved with the range picker, and every
"coverage gap" was one rival's own exclusive. Each block below replays one of
those against a fixture with a fixed today.

    python social_dashboard/test_competitors.py
"""

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

print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
