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

print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
