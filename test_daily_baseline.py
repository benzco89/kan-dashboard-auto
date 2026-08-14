"""Locks the daily baseline — specifically, that it compares like with like.

Until 2026-08-14 the "weekly average" in the daily report summed the CUMULATIVE
views of every post published in the last 7 days and divided by 7, then held
yesterday's ~20-hour-old posts up against it. Measured against the sheet that
morning, yesterday came out at 0.60x the baseline on YouTube, 0.81x on
Facebook, 0.77x on Instagram and 0.27x on TikTok — and a snapshot from 26/07
replayed against the same posts today showed why: an Instagram post holds 74%
of its 7-day value after one day, a YouTube video 36%. "יום חלש" was the
default output, not a finding.

The sheet keeps no per-day history to reconstruct from (`views_delta` is a
single column, overwritten every run, and is 0 on a post's first capture), so
the report records its own basis: yesterday's totals go into "בסיס יומי" every
morning, and the baseline is the mean of the days already recorded there — each
one measured at the same age as the day being judged.

    python test_daily_baseline.py
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import telegram_reporter as tr  # noqa: E402

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


def hist(*pairs):
    """history rows: (date, yt) -> a row with the other platforms filled in."""
    return [{"date": d, "yt_views": yt, "fb_reach": yt * 2,
             "ig_views": yt * 3, "tt_views": yt * 4} for d, yt in pairs]


print("\nday totals\n" + "-" * 62)

yt = pd.DataFrame([{"published_at": "2026-08-13", "views": 100},
                   {"published_at": "2026-08-13", "views": 50},
                   {"published_at": "2026-08-12", "views": 900}])
fb = pd.DataFrame([{"date": "2026-08-13", "reach": 70, "views": 5},
                   {"date": "2026-08-11", "reach": 900, "views": 5}])
ig = pd.DataFrame([{"date": "2026-08-13", "views": 30}])
tt = pd.DataFrame([{"date": "2026-08-13", "views": 12}])

t = tr.day_totals(yt, fb, ig, tt, "2026-08-13")
check("youtube sums only that day's videos", t["yt_views"], 150)
check("facebook uses reach, not views", t["fb_reach"], 70)
check("instagram sums that day", t["ig_views"], 30)
check("tiktok sums that day", t["tt_views"], 12)

# the collectors write dates as datetimes often enough to matter
t2 = tr.day_totals(pd.DataFrame([{"published_at": "2026-08-13 20:14", "views": 7}]),
                   pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "2026-08-13")
check("a timestamp still lands on its day", t2["yt_views"], 7)
check("an empty frame is 0, not a crash", t2["fb_reach"], 0)

t3 = tr.day_totals(pd.DataFrame([{"published_at": "2026-08-13"}]),
                   pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "2026-08-13")
check("a missing metric column is 0", t3["yt_views"], 0)


print("\nbaseline from recorded history\n" + "-" * 62)

rows = hist(("2026-08-06", 10), ("2026-08-07", 20), ("2026-08-08", 30),
            ("2026-08-09", 40), ("2026-08-10", 50), ("2026-08-11", 60),
            ("2026-08-12", 70), ("2026-08-13", 999))

means, used = tr.baseline_from_history(rows, "2026-08-13")
check("the judged day is not in its own average", used, 7)
check("mean of the 7 days before it", means["yt_views"], 40)
check("every platform averaged the same way", means["fb_reach"], 80)

# the window really is 7 days: 06/08 drops out when the anchor moves on
means2, used2 = tr.baseline_from_history(rows, "2026-08-14")
check("window slides with the anchor", used2, 7)
check("...and 06/08 fell out of it", means2["yt_views"], (20 + 30 + 40 + 50 + 60 + 70 + 999) // 7)

# a re-run, or a hand edit, can leave two rows for one day
dupes = hist(("2026-08-10", 50), ("2026-08-11", 60)) + hist(("2026-08-11", 6000))
means3, used3 = tr.baseline_from_history(dupes, "2026-08-12", min_days=2)
check("a duplicated day counts once", used3, 2)
check("...and the later row wins", means3["yt_views"], (50 + 6000) // 2)

# a run that wrote nothing is not a day of data
broken = hist(("2026-08-10", 50), ("2026-08-11", 60))
broken.append({"date": "2026-08-12", "yt_views": 0, "fb_reach": 0,
               "ig_views": 0, "tt_views": 0})
means4, used4 = tr.baseline_from_history(broken, "2026-08-13", min_days=2)
check("an all-zero row is a failed run, not a quiet day", used4, 2)
check("...so it does not drag the mean down", means4["yt_views"], 55)

# a genuine zero on ONE platform is real data and must survive
quiet = hist(("2026-08-10", 50), ("2026-08-11", 60))
quiet.append({"date": "2026-08-12", "yt_views": 0, "fb_reach": 120,
              "ig_views": 180, "tt_views": 240})
means5, used5 = tr.baseline_from_history(quiet, "2026-08-13", min_days=2)
check("a day with no YouTube uploads still counts", used5, 3)
check("...and its zero is averaged in", means5["yt_views"], (50 + 60 + 0) // 3)

check("blank cells parse as 0", tr.baseline_from_history(
    hist(("2026-08-10", 50), ("2026-08-11", 60)) +
    [{"date": "2026-08-12", "yt_views": "", "fb_reach": "1,200",
      "ig_views": None, "tt_views": 240}], "2026-08-13", min_days=2)[0]["fb_reach"],
      (100 + 120 + 1200) // 3)


print("\nwarm-up: no invented comparison\n" + "-" * 62)

means6, used6 = tr.baseline_from_history(hist(("2026-08-12", 70)), "2026-08-13")
check("one recorded day is not a baseline", means6, None)
check("...but it is reported as one day", used6, 1)
check("no history at all", tr.baseline_from_history([], "2026-08-13"), (None, 0))

means7, used7 = tr.baseline_from_history(
    hist(("2026-08-10", 30), ("2026-08-11", 40), ("2026-08-12", 50)), "2026-08-13")
check("three days is enough to compare", used7, 3)
check("...and it averages those three", means7["yt_views"], 40)

warm = tr.format_baseline_summary(None, 1, {"yt_views": 5, "fb_reach": 5,
                                            "ig_views": 5, "tt_views": 5})
check("the warm-up text refuses the comparison",
      "אל תקבע" in warm and "1" in warm, True)
check("...and offers no per-platform average", "צפיות/יום" in warm, False)

full = tr.format_baseline_summary({"yt_views": 40, "fb_reach": 80, "ig_views": 120,
                                   "tt_views": 160}, 7,
                                  {"yt_views": 20, "fb_reach": 90, "ig_views": 120,
                                   "tt_views": 5})
check("the full text names the basis", "7" in full, True)
check("...and carries the averages", "40" in full and "160" in full, True)
check("...and yesterday's own numbers alongside them", "20" in full and "90" in full, True)


print("\nupsert: a re-run must not double-count\n" + "-" * 62)

rows8 = hist(("2026-08-11", 60), ("2026-08-12", 70))
check("a new day appends", tr.find_baseline_row(rows8, "2026-08-13"), -1)
check("a recorded day is found", tr.find_baseline_row(rows8, "2026-08-12"), 1)
check("dates are matched on the day only",
      tr.find_baseline_row([{"date": "2026-08-12 08:30"}], "2026-08-12"), 0)


print("\nsheet writes use the gspread 6 argument order\n" + "-" * 62)


class FakeWS:
    def __init__(self):
        self.calls = []

    def update(self, values, range_name=None, **kw):
        self.calls.append((values, range_name))
        return {}


ws = FakeWS()
tr.write_rows(ws, "A5", [["2026-08-13", 1, 2, 3, 4, "08:30"]])
values, rng = ws.calls[0]
check("values go first, as rows", values, [["2026-08-13", 1, 2, 3, 4, "08:30"]])
check("the range goes second", rng, "A5")

# the whole point: the pre-6 order would hand a string to `values`
check("a range string never lands in values", isinstance(values, str), False)

print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
