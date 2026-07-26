"""Replays the three real collector failures of 2026-07-26 against the checker.

A guard that does not catch the thing it was built for is worse than none, so
each case here is a mutation of a real sheet snapshot reproducing an actual
incident — and the checker has to fail on every one.

    python test_verify_collector.py
"""

import io
import sys
import contextlib
import importlib.util

spec = importlib.util.spec_from_file_location("vc", "verify_collector.py")
vc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vc)

PASS = FAIL = 0

HEADER = ["post_id", "date", "views", "reach", "likes", "shares", "pulled_at"]
GOOD = [HEADER] + [
    [f"p{i}", "2026-07-2%d" % (i % 7), str(1000 + i), str(500 + i),
     str(10 + i), str(i), "2026-07-26 08:30"]
    for i in range(1, 41)
]


def check(name, mutate, want_caught=True):
    """Run the checker over a mutated copy and assert whether it complained."""
    global PASS, FAIL
    rows = [list(r) for r in GOOD]
    mutate(rows)
    vc.fetch = lambda _k: rows
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = vc.check("facebook")
    caught = rc == 1
    ok = caught == want_caught
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    verdict = "PASS" if ok else "FAIL"
    print(f"  {verdict}  {name}" + ("" if ok else f"   (caught={caught}, want={want_caught})"))
    return buf.getvalue()


def _snapshot_good():
    """The 'before' side is whatever fetch returns at snap time."""
    vc.fetch = lambda _k: [list(r) for r in GOOD]
    with contextlib.redirect_stdout(io.StringIO()):
        vc.snap("facebook")


print("\nreplaying the incidents\n")
_snapshot_good()

# 2026-07-26: two new columns written as 0 on every row, because _insight ran a
# dict through a helper that sums it. The workflow was green.
check("a new column that arrives empty on every row",
      lambda rows: (rows[0].append("retention_end"),
                    [r.append("0") for r in rows[1:]]))

# 2026-07-26: a column inserted mid-header while rows stay positional, sliding
# 230 historical rows under the wrong names. The workflow was green.
check("a column inserted in the middle of a positional header",
      lambda rows: rows[0].insert(3, "new_col"))

# 2026-07-26: a manual run without GETXAPI_KEY / TIKHUB_TOKEN blanked that day's
# values in place. The workflow was green.
check("values wiped from existing rows",
      lambda rows: [r.__setitem__(5, "") for r in rows[1:15]])

check("a column removed outright",
      lambda rows: ([r.pop(4) for r in rows]))

print("\nand it must stay quiet when nothing is wrong\n")
check("an untouched sheet", lambda rows: None, want_caught=False)
check("new rows appended (a normal collector run)",
      lambda rows: rows.append(["p99", "2026-07-26", "1", "1", "1", "1", "2026-07-26 09:00"]),
      want_caught=False)

print(f"\n{PASS}/{PASS + FAIL} passed\n")
sys.exit(1 if FAIL else 0)
