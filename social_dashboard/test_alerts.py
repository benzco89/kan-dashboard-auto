"""Locks the vanished-post rule.

Every condition in it is an off-by-one waiting to happen: the window edge, and
"one miss is a hiccup, two is a disappearance". Both are asserted here against
hand-built rows, because the point is the boundary, not the data.

    python social_dashboard/test_alerts.py
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


def row(published, seen, title="פוסט", views=1000):
    return {"date": published, "pulled_at": seen + " 08:30", "title": title,
            "permalink": "https://x.com/1", "views": views, "type": "Reel"}


# The newest pulled_at in the sheet is what "the last run" means, so every case
# below is relative to a run on 2026-07-26.
LAST_RUN = row("2026-07-26", "2026-07-26", "seen today")


def fired(*rows):
    return [a["title"] for a in aggregate._vanished_alerts([LAST_RUN, *rows], "facebook")]


print("\nthe two-miss rule")
check("missed 2 days -> fires", fired(row("2026-07-24", "2026-07-24", "gone")), ["gone"])
check("missed 5 days -> fires", fired(row("2026-07-21", "2026-07-21", "long gone")), ["long gone"])
check("missed 1 day -> silent (an API hiccup is not a deletion)",
      fired(row("2026-07-25", "2026-07-25", "hiccup")), [])
check("seen on the last run -> silent", fired(row("2026-07-22", "2026-07-26", "alive")), [])

print("\nthe window edge — outside it, absence means nothing")
check("published 6 days ago -> still inside, fires",
      fired(row("2026-07-20", "2026-07-20", "in")), ["in"])
check("published 7 days ago -> outside, silent",
      fired(row("2026-07-19", "2026-07-19", "out")), [])

print("\nshape")
alerts = aggregate._vanished_alerts([LAST_RUN, row("2026-07-22", "2026-07-22")], "facebook")
a = alerts[0]
check("severity is high", a["severity"], "high")
check("kind", a["kind"], "vanished")
check("last-seen date in the value column", a["value"], "07/22")
check("longer gone ranks higher",
      aggregate._vanished_alerts([LAST_RUN, row("2026-07-21", "2026-07-21")], "facebook")[0]["_impact"] >
      aggregate._vanished_alerts([LAST_RUN, row("2026-07-24", "2026-07-24")], "facebook")[0]["_impact"], True)

print("\nedges")
check("empty sheet", aggregate._vanished_alerts([], "facebook"), [])
check("no pulled_at anywhere", aggregate._vanished_alerts([{"date": "2026-07-24"}], "facebook"), [])
check("a t.co link is not a headline",
      aggregate._clean_alert_title("תיעוד מהזירה https://t.co/abc @kann_news"), "תיעוד מהזירה")

print(f"\n{PASS}/{PASS + FAIL} passed\n")
sys.exit(1 if FAIL else 0)
