"""
Read-only calibration tool for the alerts/anomaly thresholds in aggregate.py.

Prints, per platform, the live distribution (percentiles) of every metric the
alerts engine keys on, then runs the real engine and reports how many alerts
each (kind, platform) produces per window. Use it to re-tune the
_HIT / _SPREAD / _SAVE_RATE / _HOOK_SKIP constants if the account profile shifts.

    PYTHONPATH=. venv/Scripts/python.exe analyze_thresholds.py

Never writes to Sheets — it only reads the same data the dashboard serves.
"""

from collections import Counter

import aggregate as A
import gsheets


def pct(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    frac = k - lo
    return xs[lo] * (1 - frac) + xs[hi] * frac


def line(name, xs):
    if not xs:
        print("  %-16s n=0" % name)
        return
    print("  %-16s n=%3d  p50=%9.2f  p75=%9.2f  p90=%10.2f  p95=%10.2f  p99=%11.2f  max=%11.2f"
          % (name, len(xs), pct(xs, 50), pct(xs, 75), pct(xs, 90), pct(xs, 95), pct(xs, 99), max(xs)))


def distributions(data, days):
    start, end, _p1, _p2 = A._window(days)
    print("\n============ DISTRIBUTIONS - %d days (%s..%s) ============" % (days, start, end))
    for plat in A._ALERT_PLATFORMS:
        posts = A._norm_posts(data[plat], plat, start, end)
        print("\n### %s  (%d posts with views>0)" % (plat, len(posts)))
        med_v = A._median([p["views"] for p in posts])
        line("views", [p["views"] for p in posts])
        line("views/median", [p["views"] / med_v for p in posts] if med_v else [])
        line("eng% int/view", [p["eng"] for p in posts if p["eng"] > 0])
        sh = [p for p in posts if p["shares"] > 0]
        line("shares(abs)", [p["shares"] for p in sh])
        line("share_rate%", [p["share_rate"] for p in sh])
        if plat == "instagram":
            sv = [p for p in posts if p["saved"] > 0]
            line("saved(abs)", [p["saved"] for p in sv])
            line("save_rate%", [p["save_rate"] for p in sv])
            line("reel skip%", [p["skip"] for p in posts if "reel" in str(p["raw_type"]).lower() and p["skip"] > 0])

    print("\n### followers  daily |change| over %d days" % days)
    rows = [r for r in data["followers"] if A._in_range(A._parse_date(r.get("date")), start, end)]
    for plat, ckey in A._FOLLOWER_KEYS:
        vals = [abs(A._num(r.get(ckey))) for r in rows
                if str(r.get(ckey) or "").strip() != "" and A._num(r.get(ckey)) != 0]
        line(plat, vals)


def simulate(data, days):
    """Run the real engine and report the (kind, platform) breakdown it produces."""
    out = A.build_alerts(data, days)
    s = out["summary"]
    print("\n============ SIMULATED FEED - %d days ============" % days)
    print("  total=%d  high=%d  | hits=%d spread=%d saves=%d flops=%d hooks=%d followers=%d"
          % (s["total"], s["high"], s["hits"], s["spread"], s["saves"], s["flops"], s["hooks"], s["followers"]))
    for (k, p), n in sorted(Counter((a["kind"], a["platform"]) for a in out["alerts"]).items()):
        print("    %-14s %-10s %d" % (k, p, n))


def main():
    data = gsheets.get_data()
    for days in (7, 30, 90):
        distributions(data, days)
    for days in (7, 30):
        simulate(data, days)


if __name__ == "__main__":
    main()
