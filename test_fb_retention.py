"""Locks the retention maths in facebook_collector.

Every curve below is a REAL one, copied out of the probe run on 2026-07-26
(`gh run view` on probe_fb_video.yml), because the whole point of the metric is
what it says about actual Kan reels — a synthetic curve would have hidden the
one thing that matters: a bucket is ~0.9s on a short reel and 21s on a long
video, so "the second bucket" measures duration, not attention.

    python test_fb_retention.py
"""

import sys
import importlib.util

spec = importlib.util.spec_from_file_location("fbc", "facebook_collector.py")
fbc = importlib.util.module_from_spec(spec)
sys.modules["fbc"] = fbc
try:
    spec.loader.exec_module(fbc)
except SystemExit:
    pass        # the collector exits without credentials; we only want its helpers

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


def curve(*vals):
    return {str(i): v for i, v in enumerate(vals)}


# "מצאנו את הילד, אני פה עם הסוס" — 19.633s, 21 buckets (0.93s each)
KOGAN = curve(.998, .999, .999, .999, .915, .850, .806, .768, .742, .723, .707,
              .693, .680, .667, .660, .653, .646, .639, .632, .627, .608)
# עמיר בניון — 858.766s, 41 buckets (20.95s each)
LONG = curve(.998, .114, .081, .071, .062, .052, .041, .037, .037, .034, .030,
             .029, .028, .027, .025, .023, .024, .023, .024, .021, .018, .019,
             .018, .019, .018, .017, .017, .016, .016, .015, .015, .014, .014,
             .013, .013, .012, .012, .011, .010, .008, .005)
# נרצחו בלוד — 36.16s, 41 buckets (0.88s each)
LOD = curve(.997, .998, .998, .998, .891, .772, .666, .592, .532, .489, .448,
            .410, .379, .350, .320, .294, .269, .250, .232, .218, .206, .192,
            .184, .180, .176, .172, .168, .164, .160, .156, .152, .148, .144,
            .140, .136, .132, .128, .124, .118, .110, .103)

print("\nshort reels — a bucket is under 3s, so the 3-second reading is real")
check("Kogan clip holds 60.8% to the end", fbc._retention_points(KOGAN, 19.633)[1], 60.8)
check("...and 99.9% at 3 seconds", fbc._retention_points(KOGAN, 19.633)[0], 99.9)
check("Lod reel ends at 10.3%", fbc._retention_points(LOD, 36.16)[1], 10.3)
check("Lod reel at 3 seconds", fbc._retention_points(LOD, 36.16)[0], 99.8)

print("\nlong video — 21s per bucket, so 3 seconds cannot be read at all")
check("end is still reported", fbc._retention_points(LONG, 858.766)[1], 0.5)
check("3s reports 0 (unmeasured), NOT the misleading 99.8", fbc._retention_points(LONG, 858.766)[0], 0)

print("\nedges")
check("no length -> only the end is known", fbc._retention_points(KOGAN, 0), (0, 60.8))
check("empty curve", fbc._retention_points({}, 20), (0, 0))
check("single bucket", fbc._retention_points(curve(.5), 20), (0, 50.0))
check("garbage keys do not raise", fbc._retention_points({"a": .5}, 20), (0, 0))

print("\nthe stored curve — two numbers say whether they stayed, only the curve says where they left")
check("packs as per-mille integers", fbc._pack_curve(curve(.998, .915, .608)), "998,915,608")
check("keeps every bucket", len(fbc._pack_curve(KOGAN).split(",")), 21)
check("empty curve stores nothing", fbc._pack_curve({}), "")
check("garbage keys store nothing", fbc._pack_curve({"a": 1}), "")
check("a non-video post still carries every key",
      sorted(fbc.get_video_insights(None)),
      ['avg_watch_sec', 'duration_sec', 'plays', 'replays', 'retention_3s',
       'retention_curve', 'retention_end', 'total_plays', 'total_watch_min',
       'views_30s'])

print(f"\n{PASS}/{PASS + FAIL} passed\n")
sys.exit(1 if FAIL else 0)
