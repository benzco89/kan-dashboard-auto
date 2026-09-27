"""Locks the intraday competitors run — the parts that run without a network.

The run writes production sheets five times a day, so what it may touch is
checked here rather than trusted: it must never write a Kan tab (their _delta
columns are diffs against the morning pull) or the daily followers snapshot.

    python test_competitors_intraday.py
"""

import sys
from datetime import datetime

import competitors_intraday as CI
import competitors_collector as CC

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


print("\nwhat the run may write\n" + "-" * 62)
check("never a Kan tab", set(CI.OUR_TABS.values()) & set(CI.WRITTEN_TABS), set())
check("never the daily followers snapshot", CC.SHEET_NAME in CI.WRITTEN_TABS, False)
try:
    CI.write_tab(None, "נתוני אינסטגרם", [], ["a"])
    refused = False
except ValueError:
    refused = True
check("write_tab refuses any other tab before touching the sheet", refused, True)

print("\nhistory\n" + "-" * 62)
h = CI.history_rows([{"post_id": 17, "username": "aaa", "date": "2026-09-27", "time": "09:30",
                      "likes": 10, "comments": 2}], "2026-09-27 14:00")
check("post id is a string", h[0]["post_id"], "17")
check("age at the pull, in hours", h[0]["age_h"], 4.5)
check("counts and times are kept", (h[0]["likes"], h[0]["comments"], h[0]["posted_at"], h[0]["pulled_at"]),
      (10, 2, "2026-09-27 09:30", "2026-09-27 14:00"))
check("columns cover every field", sorted(h[0]), sorted(CI.HISTORY_COLUMNS))

NOW = datetime(2026, 9, 27, 14, 0)
kept = CI.keep_recent([{"pulled_at": "2026-09-19 23:00"}, {"pulled_at": "2026-09-20 11:00"},
                       {"pulled_at": ""}], "pulled_at", 7, NOW)
check("rows past the retention are dropped", [r["pulled_at"] for r in kept], ["2026-09-20 11:00"])

print("\nKan's fresh posts\n" + "-" * 62)
k = CI.kan_fresh_rows(
    [{"timestamp": "2026-09-27T06:30:00+0000", "caption": "רעידת אדמה"},
     {"timestamp": "2026-09-26T22:30:00+0000", "caption": None}],
    [{"created_time": "2026-09-27T08:00:00+0000", "message": "כותרת"}])
check("instagram rows use the sheet's caption column", k["instagram"][0], {"date": "2026-09-27", "caption": "רעידת אדמה"})
check("a post after midnight Israel time is dated in Israel", k["instagram"][1]["date"], "2026-09-27")
check("missing text becomes empty", k["instagram"][1]["caption"], "")
check("facebook rows use the sheet's title column", k["facebook"], [{"date": "2026-09-27", "title": "כותרת"}])

print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
