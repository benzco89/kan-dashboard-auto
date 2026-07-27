"""Locks the account-level daily block.

The rule that matters is which day a number belongs to. Meta closes its day at
10:00 Israel and the collector runs at 08:30, so until 2026-07-27 the figure in
a row described a different day than the row's own date — the same reach landed
in two consecutive rows. `insights_day` now carries the answer, and anything
without it is from before the fix and must be dropped rather than drawn a day
off. That, and "one point is not a chart", are what these assertions hold.

    python social_dashboard/test_account_daily.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import aggregate  # noqa: E402

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


def row(date, day=None, **vals):
    r = {"date": date, "insights_day": day or ""}
    r.update(vals)
    return r


print("\naccount-daily block\n" + "-" * 60)

# --- the day comes from insights_day, never from the row ---
rows = [
    row("2026-07-26", None, fb_page_views=99999, fb_daily_reach=1080464),   # pre-fix
    row("2026-07-27", "2026-07-26", fb_page_views=18282, fb_daily_reach=1134262),
]
ad = aggregate._account_daily(rows, "facebook")
check("dated by insights_day, not by the row", ad["day"], "2026-07-26")
check("the pre-fix row is dropped", ad["days_collected"], 1)
check("first tile is page views", ad["stats"][0]["value"], 18282)
check("reach comes from the same row", ad["stats"][1]["value"], 1134262)

# a row whose day is unknown must not be plotted one day off — even a big number
check("an undated row contributes nothing", 99999 in [s["value"] for s in ad["stats"]], False)

# --- one point is not a chart ---
check("no history on day one", ad["history"], [])
two = rows + [row("2026-07-28", "2026-07-27", fb_page_views=17000)]
check("no history on day two", aggregate._account_daily(two, "facebook")["history"], [])
three = two + [row("2026-07-29", "2026-07-28", fb_page_views=16000)]
ad3 = aggregate._account_daily(three, "facebook")
check("history from day three", len(ad3["history"]), 3)
check("history is in date order", [p["day"] for p in ad3["history"]],
      ["2026-07-26", "2026-07-27", "2026-07-28"])
check("the latest day still wins", ad3["day"], "2026-07-28")

# --- an all-zero day is not a day ---
zeros = [row("2026-07-27", "2026-07-26", fb_page_views=0, fb_daily_reach=0)]
check("a row of zeros is skipped", aggregate._account_daily(zeros, "facebook"), None)
check("nothing at all -> None", aggregate._account_daily([], "facebook"), None)

# --- Instagram carries its own four ---
ig = [row("2026-07-27", "2026-07-26", ig_profile_views=3157, ig_accounts_engaged=30069,
          ig_daily_reach=428970, ig_daily_views=1606829)]
adi = aggregate._account_daily(ig, "instagram")
check("instagram leads with profile views", adi["stats"][0]["value"], 3157)
check("instagram carries accounts engaged", adi["stats"][1]["value"], 30069)
check("four tiles per platform", len(adi["stats"]), 4)

# --- history is bounded ---
many = [row("2026-08-%02d" % i, "2026-08-%02d" % (i - 1), fb_page_views=1000 + i)
        for i in range(2, 40)]
check("history capped at 30 points", len(aggregate._account_daily(many, "facebook")["history"]), 30)

print("-" * 60)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
