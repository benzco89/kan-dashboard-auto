"""Locks the YouTube merge — specifically, that it stops eating columns.

`youtube_lifetime_refresh` writes `views_lifetime` into the sheet. The collector
then rebuilds the whole sheet every morning, and its dedupe keeps the NEW row,
which has no such column: concat fills NaN and the fillna(0) downstream turns it
into a zero. Every video under 30 days old would lose the value daily, with no
error anywhere — the same shape as the four follower columns that stayed empty
for eight months.

    python test_youtube_merge.py
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('YOUTUBE_API_KEY', 'test')
import youtube_collector  # noqa: E402

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


merge = youtube_collector.merge_with_existing

print("\nyoutube merge\n" + "-" * 62)

# a video the collector still refreshes, whose lifetime figure was written by
# the weekly refresh
new = pd.DataFrame([{"video_id": "a", "views": 1000, "title": "טרי"}])
old = pd.DataFrame([{"video_id": "a", "views": 900, "title": "טרי",
                     "views_lifetime": 1500, "lifetime_checked": "2026-07-27"}])
m = merge(new, old).set_index("video_id")
check("the fresh view count wins", int(m.loc["a", "views"]), 1000)
check("views_lifetime survives", int(m.loc["a", "views_lifetime"]), 1500)
check("its date survives too", m.loc["a", "lifetime_checked"], "2026-07-27")

# a video that has aged out: only the old row exists, nothing to overwrite
new2 = pd.DataFrame([{"video_id": "b", "views": 50, "title": "חדש"}])
old2 = pd.DataFrame([{"video_id": "z", "views": 10, "title": "ישן", "views_lifetime": 99}])
m2 = merge(new2, old2).set_index("video_id")
check("an aged-out row is untouched", int(m2.loc["z", "views_lifetime"]), 99)
check("and the new video is added", int(m2.loc["b", "views"]), 50)
check("a new video has no lifetime yet", pd.isna(m2.loc["b", "views_lifetime"]), True)

# the original direction still holds: a column only the collector produces
new3 = pd.DataFrame([{"video_id": "a", "views": 5, "views_delta": 2}])
old3 = pd.DataFrame([{"video_id": "q", "views": 1}])
m3 = merge(new3, old3)
check("a collector-only column is added to old rows", "views_delta" in m3.columns, True)

# duplicates in the sheet must not blow up the index
new4 = pd.DataFrame([{"video_id": "a", "views": 7}])
old4 = pd.DataFrame([{"video_id": "a", "views": 6, "views_lifetime": 60},
                     {"video_id": "a", "views": 5, "views_lifetime": 50}])
m4 = merge(new4, old4).set_index("video_id")
check("duplicate ids in the sheet are survivable", int(m4.loc["a", "views_lifetime"]), 60)

# empty history
check("no history -> the new frame as-is", len(merge(new, pd.DataFrame())), 1)

print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
