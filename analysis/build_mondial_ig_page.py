# -*- coding: utf-8 -*-
"""מזריק את נתוני האינסטגרם לתבנית ומייצר את עמוד הממצאים."""
import csv, json, collections, datetime, os, sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def _int(v):
    try:
        return int(str(v).replace(",", "") or 0)
    except (ValueError, TypeError):
        return 0


ig = list(csv.DictReader(open(os.path.join(HERE, "mondial_instagram_2026.csv"), encoding="utf-8-sig")))
tw = list(csv.DictReader(open(os.path.join(HERE, "mondial_tweets_2026.csv"), encoding="utf-8-sig")))
caps = list(csv.DictReader(open(os.path.join(
    ROOT, "ig_captions", "captions_2026-06-08_2026-08-11.csv"), encoding="utf-8-sig")))

posts = sorted(({"d": r["תאריך"], "t": r["שעה"], "v": r["וידאו"] == "כן", "k": r["סוג"],
                 "x": r["כיתוב"], "u": r["קישור"], "n": _int(r["צפיות"]),
                 "re": _int(r["רשומים"]), "l": _int(r["לייקים"]), "c": _int(r["תגובות"]),
                 "cut": r["נקטע בגיליון"] == "כן"} for r in ig),
               key=lambda a: (a["d"], a["t"]))

lo, hi = datetime.date(2026, 6, 8), datetime.date(2026, 8, 11)
cnt, days, d = collections.Counter((a["d"], a["v"]) for a in posts), [], lo
while d <= hi:
    k = d.isoformat()
    days.append({"d": k, "reel": cnt[(k, True)], "still": cnt[(k, False)]})
    d += datetime.timedelta(days=1)

# היסטוגרמה של אורכי כיתוב, בסלים של 100 תווים - כדי להראות איפה הגיליון חותך
BUCKET, TOP = 100, 1500
hist = collections.Counter()
for r in caps:
    n = int(r["caption_len"])
    hist[min(n // BUCKET * BUCKET, TOP)] += 1
buckets = [{"from": b, "n": hist.get(b, 0)} for b in range(0, TOP + BUCKET, BUCKET)]

# כמה פוסטים שנבחרו לטבלה היו מתפספסים אילו חיפשנו בכיתוב הקטום של הגיליון.
# נמדד ולא נכתב ביד, אחרת המספר הזה יישאר 0 גם ביום שבו הוא כבר לא.
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)
import mondial_lexicon as _L  # noqa: E402
from social_dashboard import gsheets  # noqa: E402

_sheet = {r["media_id"]: r for r in gsheets.get_data(keys=["instagram"])["instagram"]}
_full = {r["media_id"]: r for r in caps}
_picked = {r["media_id"] for r in ig}
hidden = sum(
    1 for mid in _picked
    if mid in _full
    and _L.signals(_full[mid]["caption_full"])[0]
    and not _L.signals((_sheet.get(mid) or {}).get("caption", "") or "")[0]
)

med = lambda xs: sorted(xs)[len(xs) // 2] if xs else 0
ig_views = [_int(r["צפיות"]) for r in ig]
tw_views = [_int(r["צפיות"]) for r in tw]

data = {
    "posts": posts, "days": days, "buckets": buckets, "bucket": BUCKET, "top": TOP,
    "stats": {
        "total": len(ig), "reels": sum(1 for a in posts if a["v"]),
        "views": sum(ig_views), "cut": sum(1 for a in posts if a["cut"]),
        "types": dict(collections.Counter(a["k"] for a in posts)),
    },
    "captions": {
        "n": len(caps),
        "over": sum(1 for r in caps if int(r["caption_len"]) > 500),
        "longest": max(int(r["caption_len"]) for r in caps),
        "hidden": hidden,
    },
    "compare": {
        "ig": {"items": len(ig), "views": sum(ig_views), "median": med(ig_views),
               "video": sum(1 for r in ig if r["וידאו"] == "כן")},
        "tw": {"items": len(tw), "views": sum(tw_views), "median": med(tw_views),
               "video": sum(1 for r in tw if r["וידאו"] == "כן")},
    },
    "phases": [{"from": "2026-06-11", "to": "2026-06-27", "label": "שלב הבתים"},
               {"from": "2026-06-28", "to": "2026-07-05", "label": "שמינית"},
               {"from": "2026-07-06", "to": "2026-07-12", "label": "רבע"},
               {"from": "2026-07-13", "to": "2026-07-16", "label": "חצי"},
               {"from": "2026-07-19", "to": "2026-07-19", "label": "גמר"}],
}

blob = json.dumps(data, ensure_ascii=False)
assert "�" not in blob, "טקסט עם בייטים שבורים - הצרכן יפסול את העמוד"
tpl = open(os.path.join(HERE, "mondial_ig_template.html"), encoding="utf-8").read()
out = tpl.replace("/*__DATA__*/", blob)
assert "__DATA__" not in out
open(os.path.join(HERE, "mondial_ig_page.html"), "w", encoding="utf-8").write(out)
print("✅ mondial_ig_page.html | %d פוסטים, %d רילס, %s צפיות | %d כיתובים בהיסטוגרמה"
      % (data["stats"]["total"], data["stats"]["reels"],
         format(data["stats"]["views"], ","), data["captions"]["n"]))
