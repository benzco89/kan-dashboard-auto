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
NOW = datetime(2026, 9, 27, 14, 0)


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


print("\nwhat the run may write\n" + "-" * 62)
check("never a Kan tab", set(CI.OUR_TABS.values()) & set(CI.WRITTEN_TABS), set())
check("never the daily followers snapshot", CC.SHEET_NAME in CI.WRITTEN_TABS, False)
try:
    CI.append_log(None, "נתוני אינסטגרם", [], ["a"], "a", 7, NOW)
    refused = False
except ValueError:
    refused = True
check("append_log refuses any other tab before touching the sheet", refused, True)

print("\nhistory\n" + "-" * 62)
h = CI.history_rows([{"post_id": 17, "username": "aaa", "date": "2026-09-27", "time": "09:30",
                      "likes": 10, "comments": 2}], "2026-09-27 14:00")
check("post id is a string", h[0]["post_id"], "17")
check("age at the pull, in hours", h[0]["age_h"], 4.5)
check("counts and times are kept", (h[0]["likes"], h[0]["comments"], h[0]["posted_at"], h[0]["pulled_at"]),
      (10, 2, "2026-09-27 09:30", "2026-09-27 14:00"))
check("columns cover every field", sorted(h[0]), sorted(CI.HISTORY_COLUMNS))

print("\nKan's fresh posts\n" + "-" * 62)
k = CI.kan_fresh_rows(
    [{"timestamp": "2026-09-27T06:30:00+0000", "caption": "רעידת אדמה"},
     {"timestamp": "2026-09-26T22:30:00+0000", "caption": None}],
    [{"created_time": "2026-09-27T08:00:00+0000", "message": "כותרת"}])
check("instagram rows use the sheet's caption column", k["instagram"][0], {"date": "2026-09-27", "caption": "רעידת אדמה"})
check("a post after midnight Israel time is dated in Israel", k["instagram"][1]["date"], "2026-09-27")
check("missing text becomes empty", k["instagram"][1]["caption"], "")
check("facebook rows use the sheet's title column", k["facebook"], [{"date": "2026-09-27", "title": "כותרת"}])

print("\nmain(): write order and abort\n" + "-" * 62)


def run_main(fail_candidates):
    """Runs CI.main() with every network/sheet call faked. Returns
    (call order, the exception main() raised, or None)."""
    calls = []
    saved = {
        "DRY_RUN": CI.DRY_RUN, "ACCESS_TOKEN": CI.CC.ACCESS_TOKEN,
        "get_own_ig_id": CI.CC.get_own_ig_id, "fetch_account": CI.CC.fetch_account,
        "fetch_kan_posts": CI.fetch_kan_posts, "_open": CI.CC._open,
        "save_posts": CI.CC.save_posts, "read_tab": CI.read_tab,
        "append_log": CI.append_log, "sleep": CI.time.sleep,
    }
    try:
        CI.DRY_RUN = False
        CI.CC.ACCESS_TOKEN = "test-token"
        CI.CC.get_own_ig_id = lambda: "999"
        CI.CC.fetch_account = lambda own_ig, username: (
            {"followers": 100},
            [{"post_id": f"{username}-1", "username": username, "date": "2026-09-27",
              "time": "10:00", "likes": 5, "comments": 1, "caption": "x", "permalink": ""}])
        CI.fetch_kan_posts = lambda own_ig: ([], [])
        CI.CC._open = lambda: object()
        CI.time.sleep = lambda s: None
        CI.read_tab = lambda sh, name: []

        def fake_save_posts(sh, df):
            calls.append("save_posts")
        CI.CC.save_posts = fake_save_posts

        def fake_append_log(sh, name, rows, columns, date_col, keep_days, now):
            calls.append(f"append_log:{name}")
            if fail_candidates and name == CI.CANDIDATES_SHEET:
                raise RuntimeError("boom")
        CI.append_log = fake_append_log

        try:
            CI.main()
            raised = None
        except Exception as e:  # noqa: BLE001 - main() must propagate, we only inspect it
            raised = e
        return calls, raised
    finally:
        CI.DRY_RUN = saved["DRY_RUN"]
        CI.CC.ACCESS_TOKEN = saved["ACCESS_TOKEN"]
        CI.CC.get_own_ig_id = saved["get_own_ig_id"]
        CI.CC.fetch_account = saved["fetch_account"]
        CI.fetch_kan_posts = saved["fetch_kan_posts"]
        CI.CC._open = saved["_open"]
        CI.CC.save_posts = saved["save_posts"]
        CI.read_tab = saved["read_tab"]
        CI.append_log = saved["append_log"]
        CI.time.sleep = saved["sleep"]


calls, raised = run_main(False)
check("write order: candidates, then posts, then history", calls,
      ["append_log:מועמדי פערים", "save_posts", "append_log:היסטוריית פוסטים מתחרים"])
check("no exception on the happy path", raised, None)

calls2, raised2 = run_main(True)
check("save_posts is never called when the candidates append_log raises",
      "save_posts" in calls2, False)
check("main() propagates the failure instead of swallowing it", raised2 is not None, True)

print("\nKan in the history\n" + "-" * 62)
kh = CI.kan_history_rows([
    {"id": "k1", "timestamp": "2026-09-27T06:30:00+0000", "like_count": 10, "comments_count": 2},
    {"id": "k2", "timestamp": "2026-09-27T07:00:00+0000"},
    {"timestamp": "2026-09-27T07:00:00+0000", "like_count": 5}], "2026-09-27 14:00")
check("Kan's IG counts enter the history as kan_news, dated in Israel",
      {k: kh[0][k] for k in ("post_id", "username", "posted_at", "age_h", "likes", "comments")},
      {"post_id": "k1", "username": "kan_news", "posted_at": "2026-09-27 09:30", "age_h": 4.5,
       "likes": 10, "comments": 2})
check("a hidden like count reads as 0", (kh[1]["likes"], kh[1]["comments"]), (0, 0))
check("a post without an id is skipped", len(kh), 2)

print("\nenvironment\n" + "-" * 62)
# the FACEBOOK_PAGE_ID secret exists but is empty; the workflow still sets the
# variable, and a get() default never fires for "" (first live dry run, 27.9)
import importlib
import os
os.environ["FACEBOOK_PAGE_ID"] = ""
check("an empty FACEBOOK_PAGE_ID falls back to Kan's page", importlib.reload(CI).PAGE_ID, "220634478361516")

print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
