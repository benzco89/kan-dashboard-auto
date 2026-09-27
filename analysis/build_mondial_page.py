# -*- coding: utf-8 -*-
"""מזריק את הנתונים לתבנית ומייצר את עמוד הממצאים."""
import csv, json, collections, datetime, os, sys
if hasattr(sys.stdout, "reconfigure"): sys.stdout.reconfigure(encoding="utf-8")
HERE = os.path.dirname(os.path.abspath(__file__))

def _int(v):
    try: return int(str(v).replace(",", "") or 0)
    except (ValueError, TypeError): return 0

rows = list(csv.DictReader(open(os.path.join(HERE, "mondial_tweets_2026.csv"), encoding="utf-8-sig")))
tweets = sorted(({"d": r["תאריך"], "t": r["שעה"], "v": r["וידאו"] == "כן", "m": r["סוג מדיה"],
                  "x": r["טקסט הציוץ"], "u": r["קישור"], "n": _int(r["צפיות"]),
                  "s": "new" if r["מקור"] == "advanced_search" else "sheet"} for r in rows),
                key=lambda a: (a["d"], a["t"]))

lo, hi = datetime.date(2026, 6, 8), datetime.date(2026, 8, 11)
cnt, days, d = collections.Counter((a["d"], a["s"]) for a in tweets), [], lo
while d <= hi:
    k = d.isoformat()
    days.append({"d": k, "sheet": cnt[(k, "sheet")], "new": cnt[(k, "new")]})
    d += datetime.timedelta(days=1)

data = {
    "tweets": tweets, "days": days,
    "stats": {"total": len(tweets), "video": sum(1 for a in tweets if a["v"]),
              "recovered": sum(1 for a in tweets if a["s"] == "new"),
              "views": sum(a["n"] for a in tweets)},
    "phases": [{"from": "2026-06-11", "to": "2026-06-27", "label": "שלב הבתים"},
               {"from": "2026-06-28", "to": "2026-07-05", "label": "שמינית"},
               {"from": "2026-07-06", "to": "2026-07-12", "label": "רבע"},
               {"from": "2026-07-13", "to": "2026-07-16", "label": "חצי"},
               {"from": "2026-07-19", "to": "2026-07-19", "label": "גמר"}],
    "sheetStart": "2026-06-21",
}
blob = json.dumps(data, ensure_ascii=False)
assert "\ufffd" not in blob, "טקסט עם בייטים שבורים - הצרכן יפסול את העמוד"
tpl = open(os.path.join(HERE, "mondial_page_template.html"), encoding="utf-8").read()
out = tpl.replace("/*__DATA__*/", blob)
assert "__DATA__" not in out
open(os.path.join(HERE, "mondial_page.html"), "w", encoding="utf-8").write(out)
print("✅ %s | %d ציוצים, %d עם וידאו, %d שוחזרו" % (
    "mondial_page.html", data["stats"]["total"], data["stats"]["video"], data["stats"]["recovered"]))
