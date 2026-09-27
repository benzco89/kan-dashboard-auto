"""Locks the cache warmer, without touching Google.

The warmer exists so no visitor ever pays for a cold read (measured live:
190-550ms warm against 0.3-3.6s cold). Three things have to hold, and all three
are the kind that fail quietly:

  * it refreshes only tabs somebody has actually asked for — warming all
    fourteen would undo the per-page split that made the dashboard fast;
  * a failed read must not replace good rows with an empty list, or a Sheets
    hiccup empties the dashboard;
  * importing the module must not start a thread, or every CLI and test that
    imports it spawns one.

    python social_dashboard/test_cache_warmer.py
"""

import os
import sys
import time

os.environ["CACHE_WARM"] = "0"          # no real thread in the test process
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gsheets  # noqa: E402

_real_fetch = gsheets._fetch

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


print("\ncache warmer\n" + "-" * 62)

check("importing does not start a thread", gsheets._warmer, None)
check("the opt-out is honoured", (gsheets._start_warmer(), gsheets._warmer)[1], None)

# --- only what was asked for ---
asked = {}


def fake_fetch(keys):
    asked["keys"] = sorted(keys)
    return {k: [{"row": k}] for k in keys}


gsheets._fetch = fake_fetch
gsheets._cache.clear()
gsheets._cache["facebook"] = ([{"row": "old"}], time.time() - 1000)
gsheets._cache["followers"] = ([{"row": "old"}], time.time() - 1000)
gsheets._warm_once()
check("warms exactly the cached tabs", asked["keys"], ["facebook", "followers"])
check("and replaces their rows", gsheets._cache["facebook"][0], [{"row": "facebook"}])
check("with a fresh stamp", gsheets._cache["facebook"][1] > time.time() - 5, True)

# --- an empty answer must not wipe good rows ---
gsheets._fetch = lambda keys: {k: [] for k in keys}
gsheets._cache["facebook"] = ([{"row": "good"}], time.time() - 1000)
gsheets._warm_once()
check("an empty read keeps the previous rows", gsheets._cache["facebook"][0], [{"row": "good"}])

# but a tab that was empty anyway may stay empty
gsheets._cache["hot_alerts"] = ([], time.time() - 1000)
gsheets._warm_once()
check("an already-empty tab is still allowed to be empty", gsheets._cache["hot_alerts"][0], [])

# --- nothing cached, nothing fetched ---
gsheets._cache.clear()
called = {"n": 0}


def counting_fetch(keys):
    called["n"] += 1
    return {}


gsheets._fetch = counting_fetch
gsheets._warm_once()
check("an empty cache asks for nothing", called["n"], 0)

# --- the interval leaves room before the entry expires ---
interval = max(30, gsheets._CACHE_TTL - gsheets._WARM_MARGIN)
check("refreshes before the TTL runs out", interval < gsheets._CACHE_TTL, True)
check("and not absurdly often", interval >= 30, True)

# --- a failed read is marked, never passed off as an empty sheet ---
# The competitors page used to read a failed tab as "no gaps — everything was
# covered". _fetch now says None for a failure, and the cache says what it served.
gsheets._cache.clear()
gsheets._status.clear()
gsheets._cache["instagram"] = ([{"row": "good"}], time.time() - 1000)
gsheets._fetch = lambda keys: {k: None for k in keys}
got = gsheets._load(["instagram", "competitor_posts"])
check("a failed read keeps the previous rows", got["instagram"], [{"row": "good"}])
check("and marks them stale", gsheets.source_status(["instagram"]), {"instagram": "stale"})
check("a failed read with nothing cached serves []", got["competitor_posts"], [])
check("and marks it unavailable", gsheets.source_status(["competitor_posts"]),
      {"competitor_posts": "unavailable"})
check("an unavailable tab is retried on the next request",
      gsheets._fresh("competitor_posts", time.time()), False)

gsheets._fetch = lambda keys: {k: [] for k in keys}
gsheets._load(["competitor_posts"])
check("a sheet that is really empty is ok", gsheets.source_status(["competitor_posts"]),
      {"competitor_posts": "ok"})

# the warmer marks a failure the same way
gsheets._cache["facebook"] = ([{"row": "good"}], time.time() - 1000)
gsheets._fetch = lambda keys: {k: None for k in keys}
gsheets._warm_once()
check("warmer: a failed read keeps the rows", gsheets._cache["facebook"][0], [{"row": "good"}])
check("warmer: and marks them stale", gsheets.source_status(["facebook"]), {"facebook": "stale"})

# the real _fetch turns one failing tab into None and keeps the others
class _Req:
    def __init__(self, fn):
        self.fn = fn

    def execute(self):
        return self.fn()


class _Vals:
    def batchGet(self, **kw):
        def boom():
            raise RuntimeError("400: one range is missing")
        return _Req(boom)

    def get(self, spreadsheetId, range):
        def run():
            if range == gsheets.ALL_SHEETS["competitor_posts"]:
                raise RuntimeError("boom")
            return {"values": [["a"], ["1"]]}
        return _Req(run)


class _Svc:
    def spreadsheets(self):
        return self

    def values(self):
        return _Vals()


gsheets._service = lambda: _Svc()
out = _real_fetch(["competitors", "competitor_posts"])
check("_fetch: the failing tab is None", out["competitor_posts"], None)
check("_fetch: the other tab still arrives", out["competitors"], [{"a": "1"}])

check("SheetData reports its tabs", gsheets.SheetData({"instagram": []}).source_status(),
      {"instagram": "stale"})

print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
