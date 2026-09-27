"""Locks the staleness rule — the one the dashboard was missing on 28/07.

A collector that writes nothing leaves the sheet internally consistent: every
row still agrees with every other row, and `_vanished_alerts`, which measures
against the newest stamp in that same sheet, sees nothing wrong. Twitter sat a
full day behind and the dashboard said nothing. Only the clock can tell.

The boundary that matters is the hour. The pipeline runs at 08:30, so before it
finishes, yesterday IS the freshest a healthy sheet can be — flag that and every
morning goes red on its own. After it, yesterday means a run was missed.

    python social_dashboard/test_freshness.py
"""

import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import aggregate  # noqa: E402

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


TODAY = date(2026, 7, 29)
YESTERDAY = TODAY - timedelta(days=1)


def at_hour(h):
    """מקבע את 'עכשיו' לשעה נתונה ב-29/07, בלי לגעת בשעון האמיתי."""
    aggregate._expected_stamp = lambda: TODAY if h >= aggregate.DAILY_RUN_DONE_HOUR else YESTERDAY


def rows(stamp, key="pulled_at", n=3):
    return [{key: f"{stamp} 08:35", "views": 100} for _ in range(n)]


ORIG = aggregate._expected_stamp
print("\nfreshness\n" + "-" * 62)

# --- the hour boundary: yesterday's stamp means opposite things at 08:00 and 11:00
at_hour(8)
f = aggregate.freshness(rows(YESTERDAY.isoformat()), "twitter")
check("before the run — yesterday is healthy", f["stale"], False)
check("before the run — zero days behind", f["days_behind"], 0)

at_hour(11)
f = aggregate.freshness(rows(YESTERDAY.isoformat()), "twitter")
check("after the run — yesterday is a missed run", f["stale"], True)
check("after the run — one day behind", f["days_behind"], 1)

# the 28/07 shape exactly: one missed run, caught the same day rather than
# surfacing a day late
f = aggregate.freshness(rows("2026-07-27"), "twitter")
check("the 28/07 gap — flagged", f["stale"], True)
check("the 28/07 gap — two days behind", f["days_behind"], 2)

# --- fresh data is never flagged, at any hour
for h in (8, 11):
    at_hour(h)
    f = aggregate.freshness(rows(TODAY.isoformat()), "twitter")
    check(f"today's stamp at {h}:00 — healthy", f["stale"], False)

# --- YouTube spells the column differently; reading `pulled_at` there saw nothing
at_hour(11)
yt = rows("2026-07-27", key="last_updated")
check("youtube reads last_updated", aggregate.freshness(yt, "youtube")["days_behind"], 2)
check("youtube rows are invisible under pulled_at", aggregate.freshness(yt, "twitter"), None)

# --- a sheet with no stamp at all is a shape problem, not a stale one
check("no stamp column -> None", aggregate.freshness([{"views": 1}], "twitter"), None)
check("empty sheet -> None", aggregate.freshness([], "twitter"), None)
check("unparseable stamp -> None", aggregate.freshness(rows("not-a-date"), "twitter"), None)

# --- the newest stamp wins, not the last row
mixed = rows("2026-07-20") + rows(TODAY.isoformat(), n=1) + rows("2026-07-22")
check("newest stamp wins", aggregate.freshness(mixed, "twitter")["stale"], False)

aggregate._expected_stamp = ORIG
print("-" * 62)
print(f"  {PASS} passed, {FAIL} failed\n")
sys.exit(1 if FAIL else 0)
