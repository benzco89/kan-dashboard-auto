# עמוד המתחרים — שלב 2: הריצה התוך־יומית — תוכנית ביצוע

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ספירות המתחרים ובדיקת "סיפורים שאין לנו" מתעדכנות חמש פעמים ביום, בלי לגעת בריצה היומית ובלי לגעת בלשוניות של כאן.

**Architecture:** סקריפט חדש, `competitors_intraday.py`, רץ מ־workflow משלו. טיימר בשרת מפעיל אותו, בדיוק כמו הסניפר. הסקריפט משתמש שוב ב־`fetch_account` וב־`save_posts` של האספן היומי, ובחישוב הפערים של הדשבורד (`competitors_page.gap_clusters`). כך יש מקור אחד לרשימת החשבונות ומקור אחד לאלגוריתם. הוא כותב שלוש לשוניות בלבד: "פוסטים מתחרים", ושתי לשוניות חדשות. העמוד קורא את יומן המועמדים של הריצה האחרונה מהיום. אם אין כזו, הוא מחשב מנתוני הבוקר, כמו עד היום.

**Tech Stack:** Python 3.10, gspread, pandas, GitHub Actions `workflow_dispatch`, systemd timer.

**Spec:** `docs/superpowers/specs/2026-09-27-competitors-design.md`, סעיף 2, סעיף 3.3 (מקור הנתונים לעמוד) וסעיף 6 (שלב 2).

## Global Constraints

- **לסדר הכתיבה בכל ריצה יש משמעות:** קודם "מועמדי פערים", אחר כך "פוסטים מתחרים", ובסוף "היסטוריית פוסטים מתחרים". אם כתיבת המועמדים נכשלת, הריצה יוצאת בלי לכתוב פוסטים.
- כל ריצה כותבת שורת `kind = "run"` אחת ליומן, גם כשאין מועמדים. העמוד מתייחס לריצה כקיימת רק אם יש לה שורה כזו.
- הריצה התוך־יומית לא כותבת לעולם ל"מתחרים" (צילום העוקבים) או לאף לשונית של כאן. `write_tab` מסרב לכל שם שאינו ב־`WRITTEN_TABS`.
- `competitors_collector.py` ו־`daily_update.yml` לא משתנים.
- `competitors_intraday.py` לא רץ מקומית אלא דרך ה־workflow בלבד. בדיקות היחידה שלו רצות מקומית בלי רשת.
- שמירה: היסטוריה 7 ימים, מועמדים 30 ימים.
- הרצת בדיקות: בדיקות הדשבורד עם `social_dashboard/venv/Scripts/python.exe`, מתוך `social_dashboard/`. בדיקות שורש הריפו עם `python`, מתוך שורש הריפו (שם מותקנים gspread ו־pandas).
- ה־branch: `competitors-intraday`. לא מתחייבים על main.
- כל מספר עם סימן בממשק עטוף ב־`<bdi dir="ltr">`.

## מפת קבצים

| קובץ | שינוי |
|---|---|
| `social_dashboard/competitors_page.py` | `gap_clusters` (ללא חיתוך), `_best_overlap`, `candidate_rows`, `gaps_from_log`, `freshness` עם `posts_pulled_at`, והעמוד משתמש ביומן |
| `social_dashboard/test_competitors.py` | בדיקות לכל אלה |
| `competitors_intraday.py` — **חדש** | הריצה |
| `test_competitors_intraday.py` — **חדש** | בדיקות לחלקים הטהורים ולשמירה מפני כתיבה |
| `.github/workflows/competitors_intraday.yml` — **חדש** | ה־workflow, כולל השמירה מפני ריצה יומית פעילה |
| `social_dashboard/deploy/kan-competitors-intraday.{sh,service,timer}` — **חדש** | הטיימר בשרת |
| `social_dashboard/gsheets.py`, `social_dashboard/server.py` | לשונית `gap_candidates` |
| `social_dashboard/templates/competitors.html` | שורת הטריות |
| `docs/ROADMAP.md`, המפרט | סטטוס, ושתי הבהרות |

---

### Task 1: יומן המועמדים ב־competitors_page

**Files:**
- Modify: `social_dashboard/competitors_page.py`
- Test: `social_dashboard/test_competitors.py`

**Interfaces:**
- Produces:
  - `gap_clusters(data, now) -> (list[item], int)` — כל הסיפורים, ממוינים לפי המעורבות של הפוסט המוביל, בלי חיתוך. ה־`int` הוא מספר הפוסטים שנבדקו.
    - שדות כל item: `kind` (`"missed"` או `"exclusive"`), `caption`, `date`, `time`, `n_outlets`, `total_eng`, `lead` (`username`, `eng`, `median`, `ratio`, `threshold`), `posts` (`username`, `post_id`, `eng`, `url`), `best_kan_overlap` (`shared`, `containment`).
  - `coverage_gaps(...)` — כמו היום, ועם שני שדות נוספים: `source="morning"` ו־`run_at=None`.
  - `CANDIDATE_COLUMNS`, `RUN_MARKER = "run"`, `candidate_rows(items, run_at, sources_checked) -> list[dict]`.
  - `gaps_from_log(rows, today) -> dict | None` — באותה צורה של `coverage_gaps`, עם `source="intraday"` ו־`run_at`.
  - `freshness(by_user, comp_posts=())` — מוסיף `posts_pulled_at`.

- [ ] **Step 1: כתוב את הבדיקות שנכשלות**

ב־`test_competitors.py`, מעל שלוש שורות הסיכום:

```python
print("\nintraday log\n" + "-" * 62)
items, checked = C.gap_clusters(GAP_DATA, NOW)
check("gap_clusters keeps every story, uncut", sorted(i["kind"] for i in items),
      ["exclusive", "exclusive", "missed"])
check("each post carries its id", items[0]["posts"][0]["post_id"], "x1")
check("the closest Kan post is measured",
      C._best_overlap(frozenset("אבגדה"), [frozenset("אבזח")]), {"shared": 2, "containment": 0.5})

rows = C.candidate_rows(items, "2026-09-27 11:00", "ig:live,fb:live")
check("one run marker plus one row per story", ([r["kind"] for r in rows][:1], len(rows)), (["run"], 4))
as_read = [{k: str(v) for k, v in r.items()} for r in rows]     # the sheet hands back strings
back = C.gaps_from_log(as_read, TODAY)
live = C.coverage_gaps(GAP_DATA, NOW)
shape = lambda g: {k: g[k] for k in ("caption", "date", "time", "n_outlets", "total_eng", "lead", "posts")}
check("the log reads back as what the page would have computed",
      [shape(g) for g in back["missed"] + back["exclusive"]],
      [shape(g) for g in live["missed"] + live["exclusive"]])
check("the log is marked intraday, with its run time", (back["source"], back["run_at"]),
      ("intraday", "2026-09-27 11:00"))
empty = [{k: str(v) for k, v in r.items()} for r in C.candidate_rows([], "2026-09-27 14:00", "x")]
check("a run with no stories still counts as a run", C.gaps_from_log(empty, TODAY)["missed"], [])
check("yesterday's run is not today's", C.gaps_from_log(as_read, TODAY + timedelta(days=1)), None)
later = C.candidate_rows(items[:1], "2026-09-27 14:00", "x")
check("the latest run of the day wins", len(C.gaps_from_log(rows + later, TODAY)["exclusive"]), 0)

b_log = C.build(dict(DATA, gap_candidates=rows), 7, today=TODAY, now=NOW)
check("the page serves the intraday log when there is one", b_log["gaps"]["source"], "intraday")
check("and the morning computation otherwise", b7["gaps"]["source"], "morning")
check("freshness says when the posts were last pulled", b7["freshness"]["posts_pulled_at"], "2026-09-27 23:00")
```

- [ ] **Step 2: הרץ ווודא שהבדיקות נכשלות**

Run: `cd social_dashboard && venv/Scripts/python.exe test_competitors.py`
Expected: `AttributeError: module 'competitors_page' has no attribute 'gap_clusters'`

- [ ] **Step 3: מימוש**

ב־`competitors_page.py`: הוסף `import json` מעל `from datetime import datetime, timedelta`, ומתחת ל־`FEED_KEEP_DAYS` הוסף:

```python
# יומן הריצה התוך־יומית ("מועמדי פערים"): שורת סימון לכל ריצה ושורה לכל סיפור
RUN_MARKER = "run"
CANDIDATE_COLUMNS = ["run_at", "kind", "cluster_key", "n_outlets", "posted_at", "caption",
                     "lead_username", "lead_eng", "lead_median", "lead_threshold",
                     "total_eng", "outlets", "best_kan_shared", "best_kan_containment",
                     "sources_checked"]
```

החלף את כל `coverage_gaps` (מ־`def coverage_gaps` ועד השורה שלפני `# ---------- העמוד ----------`) בקוד הזה:

```python
def _best_overlap(toks, ours):
    """החפיפה הגבוהה ביותר מול פוסט שלנו. נרשמת ביומן כדי שאפשר יהיה לכייל
    בדיעבד את סף "כוסה אצלנו" (0.3 ו־4 מילים) מול מה שעורך סימן."""
    best = (0, 0.0)
    for o in ours:
        inter = len(toks & o)
        if inter:
            best = max(best, (inter, round(inter / min(len(toks), len(o)), 2)))
    return {"shared": best[0], "containment": best[1]}


def gap_clusters(data, now):
    """כל המועמדים, מאוחדים לסיפורים, בלי חיתוך — (items, פוסטים שנבדקו).
    הריצה התוך־יומית רושמת את כולם; העמוד מציג רק את העליונים."""
    comp = data.get("competitor_posts", []) or []
    med = account_medians(comp)
    ours = our_token_sets(data, now)
    since = now - timedelta(hours=GAP_WINDOW_H)

    cands = []
    for p in comp:
        posted = _ts(p.get("date"), p.get("time"))
        u = str(p.get("username", "")).strip()
        if not posted or posted < since or not u:
            continue
        eng, m = _eng(p), med.get(u, 0)
        threshold = max(GAP_FLOOR, GAP_MULT * m)
        if eng < threshold:
            continue
        toks = A._viral_tokens(p.get("caption", ""))
        if len(toks) < A._MIN_TOKENS or _covered(toks, ours):
            continue
        cands.append({"username": u, "post_id": str(p.get("post_id", "")), "posted": posted,
                      "eng": eng, "median": m,
                      "ratio": round(eng / m, 1) if m else None, "threshold": threshold,
                      "caption": A._clean_caption(p.get("caption", ""))[:180],
                      "url": p.get("permalink", ""), "toks": toks})

    cands.sort(key=lambda c: -c["eng"])
    clusters = []
    for c in cands:
        home = next((g for g in clusters
                     if len(c["toks"] & g["toks"]) / min(len(c["toks"]), len(g["toks"]))
                     >= A._MATCH_CONTAINMENT), None)
        if home:
            home["posts"].append(c)
        else:
            clusters.append({"toks": c["toks"], "posts": [c]})

    items = []
    for g in clusters:
        lead = g["posts"][0]
        users = list(dict.fromkeys(c["username"] for c in g["posts"]))
        items.append({
            "kind": "missed" if len(users) >= 2 else "exclusive",
            "caption": lead["caption"],
            "date": lead["posted"].strftime("%Y-%m-%d"),
            "time": lead["posted"].strftime("%H:%M"),
            "n_outlets": len(users),
            "total_eng": sum(c["eng"] for c in g["posts"]),
            "lead": {k: lead[k] for k in ("username", "eng", "median", "ratio", "threshold")},
            "posts": [{"username": c["username"], "post_id": c["post_id"], "eng": c["eng"],
                       "url": c["url"]} for c in g["posts"]],
            "best_kan_overlap": _best_overlap(g["toks"], ours),
        })
    return items, len(cands)


def coverage_gaps(data, now, sources=None):
    sources = sources or {}
    unavailable = [k for k in GAP_SOURCES if sources.get(k, "ok") == "unavailable"]
    stale = [k for k in GAP_SOURCES if sources.get(k, "ok") == "stale"]
    base = {"failed": [], "stale": stale, "source": "morning", "run_at": None}
    if unavailable:
        return dict(base, status="unavailable", failed=unavailable, checked=0,
                    missed=[], exclusive=[])
    items, checked = gap_clusters(data, now)
    return dict(base, status="stale" if stale else "ok", checked=checked,
                missed=[g for g in items if g["kind"] == "missed"][:GAPS_TOP],
                exclusive=[g for g in items if g["kind"] == "exclusive"][:GAPS_TOP])


def candidate_rows(items, run_at, sources_checked):
    """שורות היומן של ריצה אחת: שורת סימון, גם כשאין מועמדים — כך העמוד יודע
    שהבדיקה רצה — ושורה לכל סיפור."""
    rows = [{"run_at": run_at, "kind": RUN_MARKER, "n_outlets": 0,
             "sources_checked": sources_checked}]
    for g in items:
        lead = g["lead"]
        rows.append({
            "run_at": run_at, "kind": g["kind"], "cluster_key": g["posts"][0]["post_id"],
            "n_outlets": g["n_outlets"], "posted_at": g["date"] + " " + g["time"],
            "caption": g["caption"], "lead_username": lead["username"],
            "lead_eng": lead["eng"], "lead_median": lead["median"],
            "lead_threshold": lead["threshold"], "total_eng": g["total_eng"],
            "outlets": json.dumps(g["posts"], ensure_ascii=False),
            "best_kan_shared": g["best_kan_overlap"]["shared"],
            "best_kan_containment": g["best_kan_overlap"]["containment"],
            "sources_checked": sources_checked,
        })
    return rows


def gaps_from_log(rows, today):
    """הריצה התוך־יומית האחרונה של היום, באותה צורה של coverage_gaps. None אם
    אין ריצה היום — אז העמוד מחשב מנתוני הבוקר, שבהם המתחרים וכאן נכונים
    לאותה שעה. בלי זה, ספירות מתחרים של 14:00 מול פוסטים שלנו מהבוקר היו
    מסמנות כפער כל סיפור שפרסמנו מאז הבוקר."""
    runs = [str(r.get("run_at", "")) for r in rows
            if r.get("kind") == RUN_MARKER and str(r.get("run_at", ""))[:10] == str(today)]
    if not runs:
        return None
    last = max(runs)
    missed, exclusive = [], []
    for r in rows:
        if str(r.get("run_at", "")) != last or r.get("kind") == RUN_MARKER:
            continue
        try:
            posts = json.loads(r.get("outlets") or "[]")
        except ValueError:
            posts = []
        eng, med = A._int(r.get("lead_eng")), A._num(r.get("lead_median"))
        posted = str(r.get("posted_at", ""))
        item = {
            "kind": r.get("kind"), "caption": r.get("caption", ""),
            "date": posted[:10], "time": posted[11:16],
            "n_outlets": A._int(r.get("n_outlets")), "total_eng": A._int(r.get("total_eng")),
            "lead": {"username": r.get("lead_username", ""), "eng": eng, "median": med,
                     "ratio": round(eng / med, 1) if med else None,
                     "threshold": A._int(r.get("lead_threshold"))},
            "posts": posts,
        }
        (missed if item["kind"] == "missed" else exclusive).append(item)
    for lst in (missed, exclusive):
        lst.sort(key=lambda g: -g["lead"]["eng"])
    return {"status": "ok", "failed": [], "stale": [], "source": "intraday", "run_at": last,
            "checked": len(missed) + len(exclusive),
            "missed": missed[:GAPS_TOP], "exclusive": exclusive[:GAPS_TOP]}
```

ב־`freshness`: שנה את החתימה ל־`def freshness(by_user, comp_posts=()):`, והוסף למילון המוחזר:

```python
        "posts_pulled_at": max((str(p.get("pulled_at", "")) for p in comp_posts), default=""),
```

ב־`build`: שנה את `"freshness": freshness(by_user),` ל־`"freshness": freshness(by_user, comp_posts),`. שנה את `"gaps": coverage_gaps(data, now, sources),` ל:

```python
        "gaps": (gaps_from_log(data.get("gap_candidates", []) or [], today)
                 or coverage_gaps(data, now, sources)),
```

- [ ] **Step 4: הרץ ווודא שהבדיקות עוברות**

Run: `cd social_dashboard && venv/Scripts/python.exe test_competitors.py`
Expected: `64/64 passed`

- [ ] **Step 5: Commit**

```bash
git add social_dashboard/competitors_page.py social_dashboard/test_competitors.py
git commit -m "competitors_page: every gap candidate, a run log that reads back, and the page prefers it"
```

---

### Task 2: הסקריפט התוך־יומי

**Files:**
- Create: `competitors_intraday.py`
- Create: `test_competitors_intraday.py`

**Interfaces:**
- Consumes:
  - מ־`competitors_collector`: `COMPETITORS`, `fetch_account(own_ig, username) -> (snapshot|None, post_rows)`, `get_own_ig_id()`, `save_posts(sh, df)`, `_open()`, `POSTS_SHEET`, `SHEET_NAME`, `BASE`, `ACCESS_TOKEN`, `IL_TZ`.
  - מ־`competitors_page` (Task 1): `gap_clusters`, `candidate_rows`, `CANDIDATE_COLUMNS`.
- Produces: `history_rows`, `keep_recent`, `kan_fresh_rows`, `write_tab`, `WRITTEN_TABS`, `OUR_TABS`, `HISTORY_COLUMNS`.

- [ ] **Step 1: כתוב את הבדיקות שנכשלות**

צור את `test_competitors_intraday.py`:

```python
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


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


print("\nwhat the run may write\n" + "-" * 62)
check("never a Kan tab", set(CI.OUR_TABS.values()) & set(CI.WRITTEN_TABS), set())
check("never the daily followers snapshot", CC.SHEET_NAME in CI.WRITTEN_TABS, False)
try:
    CI.write_tab(None, "נתוני אינסטגרם", [], ["a"])
    refused = False
except ValueError:
    refused = True
check("write_tab refuses any other tab before touching the sheet", refused, True)

print("\nhistory\n" + "-" * 62)
h = CI.history_rows([{"post_id": 17, "username": "aaa", "date": "2026-09-27", "time": "09:30",
                      "likes": 10, "comments": 2}], "2026-09-27 14:00")
check("post id is a string", h[0]["post_id"], "17")
check("age at the pull, in hours", h[0]["age_h"], 4.5)
check("counts and times are kept", (h[0]["likes"], h[0]["comments"], h[0]["posted_at"], h[0]["pulled_at"]),
      (10, 2, "2026-09-27 09:30", "2026-09-27 14:00"))
check("columns cover every field", sorted(h[0]), sorted(CI.HISTORY_COLUMNS))

NOW = datetime(2026, 9, 27, 14, 0)
kept = CI.keep_recent([{"pulled_at": "2026-09-19 23:00"}, {"pulled_at": "2026-09-20 11:00"},
                       {"pulled_at": ""}], "pulled_at", 7, NOW)
check("rows past the retention are dropped", [r["pulled_at"] for r in kept], ["2026-09-20 11:00"])

print("\nKan's fresh posts\n" + "-" * 62)
k = CI.kan_fresh_rows(
    [{"timestamp": "2026-09-27T06:30:00+0000", "caption": "רעידת אדמה"},
     {"timestamp": "2026-09-26T22:30:00+0000", "caption": None}],
    [{"created_time": "2026-09-27T08:00:00+0000", "message": "כותרת"}])
check("instagram rows use the sheet's caption column", k["instagram"][0], {"date": "2026-09-27", "caption": "רעידת אדמה"})
check("a post after midnight Israel time is dated in Israel", k["instagram"][1]["date"], "2026-09-27")
check("missing text becomes empty", k["instagram"][1]["caption"], "")
check("facebook rows use the sheet's title column", k["facebook"], [{"date": "2026-09-27", "title": "כותרת"}])

print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
```

- [ ] **Step 2: הרץ ווודא שהבדיקות נכשלות**

Run: `python test_competitors_intraday.py` (מתוך שורש הריפו)
Expected: `ModuleNotFoundError: No module named 'competitors_intraday'`

- [ ] **Step 3: מימוש**

צור את `competitors_intraday.py`:

```python
"""
Competitors intraday - רענון תוך-יומי של פיד המתחרים ובדיקת "סיפורים שאין לנו".

רץ ב-11/14/17/20/23 (טיימר ב-VPS -> workflow_dispatch, כמו הסניפר). הריצה היומית
של 08:30 לא משתנה, והיא לבדה כותבת את "מתחרים" (צילום העוקבים: אחד ביום, כי
השינוי היומי מחושב מול השורה הקודמת). כאן נכתבות רק:
  * "מועמדי פערים" - שורת סימון לכל ריצה ושורה לכל סיפור, 30 יום, כדי למדוד
    דיוק לפני שמדליקים טלגרם;
  * "פוסטים מתחרים" - אותו מיזוג לפי post_id של save_posts. ספירות טריות כל
    3 שעות: כשהפיד נמשך פעם ביום, 68% מהפוסטים של ynet קפאו לפני גיל 24 שעות;
  * "היסטוריית פוסטים מתחרים" - שורה לכל פוסט בכל ריצה, 7 ימים, לשלב 3.
לשוניות של כאן לא נכתבות לעולם: שדות ה-_delta שם הם הפרש מול המשיכה היומית.

בדיקת "אין לנו": הגיליון של כאן מעודכן רק עד הבוקר, אז פוסטי אינסטגרם ופייסבוק
של כאן נמשכים כאן חיים (טקסט ותאריך בלבד, בזיכרון) ומצטרפים לגיליון.

הסדר מכוון: מועמדים לפני פוסטים. אם כתיבת המועמדים נכשלת לא נוגעים בפוסטים,
כך שהעמוד לא מציג ספירות מתחרים מאוחרות מבדיקת הסיקור שלו.

Env: FACEBOOK_TOKEN, FACEBOOK_PAGE_ID, GCP_SERVICE_ACCOUNT.
     DRY_RUN=1 - קורא ומחשב ומדפיס, בלי לכתוב.
"""

import os
import sys
import time
from datetime import datetime, timedelta

import gspread
import pandas as pd

import competitors_collector as CC
from utils import http_get_json

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "social_dashboard"))
import competitors_page as CP  # noqa: E402

HISTORY_SHEET = "היסטוריית פוסטים מתחרים"
CANDIDATES_SHEET = "מועמדי פערים"
HISTORY_KEEP_DAYS = 7
CANDIDATES_KEEP_DAYS = 30
HISTORY_COLUMNS = ["post_id", "username", "posted_at", "pulled_at", "age_h", "likes", "comments"]
# הלשוניות היחידות שהריצה רשאית לכתוב
WRITTEN_TABS = (CANDIDATES_SHEET, CC.POSTS_SHEET, HISTORY_SHEET)
# לשוניות של כאן - קריאה בלבד, לבדיקת הכיסוי
OUR_TABS = {"instagram": "נתוני אינסטגרם", "facebook": "נתוני פייסבוק",
            "youtube": "נתוני יוטיוב", "twitter": "נתוני טוויטר", "tiktok": "נתוני טיקטוק"}
SOURCES_CHECKED = "ig:live,fb:live,yt:sheet,x:sheet,tt:sheet"
PAGE_ID = os.environ.get("FACEBOOK_PAGE_ID", "220634478361516")
DRY_RUN = os.environ.get("DRY_RUN", "").strip() not in ("", "0")


def history_rows(post_rows, pulled_at):
    """שורה לכל פוסט שנמשך: הספירה וגיל הפוסט ברגע המשיכה."""
    pulled = datetime.strptime(pulled_at, "%Y-%m-%d %H:%M")
    out = []
    for p in post_rows:
        posted_at = f"{p['date']} {p['time']}"
        posted = datetime.strptime(posted_at, "%Y-%m-%d %H:%M")
        out.append({"post_id": str(p["post_id"]), "username": p["username"],
                    "posted_at": posted_at, "pulled_at": pulled_at,
                    "age_h": round((pulled - posted).total_seconds() / 3600, 1),
                    "likes": int(p["likes"]), "comments": int(p["comments"])})
    return out


def keep_recent(rows, col, days, now):
    cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    return [r for r in rows if str(r.get(col, ""))[:10] >= cutoff and str(r.get(col, ""))]


def _il_date(ts):
    try:
        return (datetime.fromisoformat(str(ts).replace("Z", "+00:00").replace("+0000", "+00:00"))
                .astimezone(CC.IL_TZ).strftime("%Y-%m-%d"))
    except ValueError:
        return ""


def kan_fresh_rows(ig_media, fb_posts):
    """פוסטי כאן מה-API בצורת שורות הגיליון - רק מה ש-our_token_sets קורא."""
    return {
        "instagram": [{"date": _il_date(m.get("timestamp")), "caption": m.get("caption") or ""}
                      for m in ig_media],
        "facebook": [{"date": _il_date(p.get("created_time")), "title": p.get("message") or ""}
                     for p in fb_posts],
    }


def fetch_kan_posts(own_ig):
    """25 האחרונים של כאן באינסטגרם ובפייסבוק: טקסט ותאריך, קריאה אחת לכל פלטפורמה."""
    ig = http_get_json(f"{CC.BASE}/{own_ig}/media", params={
        "access_token": CC.ACCESS_TOKEN, "fields": "caption,timestamp", "limit": 25})
    fb = http_get_json(f"{CC.BASE}/{PAGE_ID}/published_posts", params={
        "access_token": CC.ACCESS_TOKEN, "fields": "message,created_time", "limit": 25})
    for name, res in (("instagram", ig), ("facebook", fb)):
        if "error" in res:
            raise RuntimeError(f"Kan {name}: {res['error'].get('message', '')[:120]}")
    return ig.get("data", []), fb.get("data", [])


def read_tab(sh, name):
    """שורות לפי כותרת, כמו gsheets בדשבורד; לשונית שאינה קיימת = []."""
    try:
        values = sh.worksheet(name).get_all_values()
    except gspread.WorksheetNotFound:
        return []
    if len(values) < 2:
        return []
    head = [h.strip() for h in values[0]]
    return [{h: (row[i] if i < len(row) else "") for i, h in enumerate(head) if h}
            for row in values[1:]]


def write_tab(sh, name, rows, columns):
    """כותב את הלשונית כולה מחדש (אותו דפוס של save_posts), ויוצר אותה אם אינה קיימת."""
    if name not in WRITTEN_TABS:
        raise ValueError(f"refusing to write {name!r}: the intraday run writes only {WRITTEN_TABS}")
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=len(rows) + 200, cols=len(columns))
    if ws.row_count < len(rows) + 1:
        ws.resize(rows=len(rows) + 200)
    ws.clear()
    ws.update([columns] + [[r.get(c, "") for c in columns] for r in rows])


def main():
    now = datetime.now(CC.IL_TZ)
    run_at = now.strftime("%Y-%m-%d %H:%M")
    print(f"\n{'=' * 50}\n🥊 Competitors intraday - {run_at}{' (DRY RUN)' if DRY_RUN else ''}\n{'=' * 50}\n")

    if not CC.ACCESS_TOKEN:
        print("❌ Missing FACEBOOK_TOKEN")
        sys.exit(1)
    own_ig = CC.get_own_ig_id()
    if not own_ig:
        print("❌ Could not resolve own IG account id")
        sys.exit(1)

    post_rows, fetched = [], 0
    for username in CC.COMPETITORS:
        snapshot, rows = CC.fetch_account(own_ig, username)   # צילום העוקבים נזרק: פעם ביום בלבד
        if snapshot:
            fetched += 1
            post_rows.extend(rows)
        time.sleep(0.3)
    print(f"📥 {fetched}/{len(CC.COMPETITORS)} accounts, {len(post_rows)} posts")
    if not fetched:
        print("❌ No accounts fetched")
        sys.exit(1)

    ig_media, fb_posts = fetch_kan_posts(own_ig)
    print(f"📥 Kan live: {len(ig_media)} IG, {len(fb_posts)} FB")

    sh = CC._open()
    data = {key: read_tab(sh, tab) for key, tab in OUR_TABS.items()}
    fresh = kan_fresh_rows(ig_media, fb_posts)
    data["instagram"] += fresh["instagram"]
    data["facebook"] += fresh["facebook"]
    merged = {str(p.get("post_id", "")): p for p in read_tab(sh, CC.POSTS_SHEET)}
    merged.update({str(p["post_id"]): p for p in post_rows})
    data["competitor_posts"] = list(merged.values())

    items, checked = CP.gap_clusters(data, now.replace(tzinfo=None))
    cand = CP.candidate_rows(items, run_at, SOURCES_CHECKED)
    missed = sum(1 for g in items if g["kind"] == "missed")
    print(f"🕳 {checked} strong posts checked -> {missed} missed, {len(items) - missed} exclusive")
    for g in items[:5]:
        print(f"   {g['kind']:9} x{g['n_outlets']} {g['lead']['username']}: {g['caption'][:70]}")

    if DRY_RUN:
        print("\n🧪 DRY RUN - nothing written")
        return

    # 1. מועמדים - לפני הפוסטים (ראו הדוקסטרינג)
    log = keep_recent(read_tab(sh, CANDIDATES_SHEET), "run_at", CANDIDATES_KEEP_DAYS, now) + cand
    write_tab(sh, CANDIDATES_SHEET, log, CP.CANDIDATE_COLUMNS)
    print(f"✅ {CANDIDATES_SHEET}: +{len(cand)} rows ({len(log)} kept)")
    # 2. פוסטים - אותו מיזוג בדיוק כמו בריצה היומית
    CC.save_posts(sh, pd.DataFrame(post_rows))
    # 3. היסטוריה
    hist = (keep_recent(read_tab(sh, HISTORY_SHEET), "pulled_at", HISTORY_KEEP_DAYS, now)
            + history_rows(post_rows, run_at))
    write_tab(sh, HISTORY_SHEET, hist, HISTORY_COLUMNS)
    print(f"✅ {HISTORY_SHEET}: +{len(post_rows)} rows ({len(hist)} kept)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: הרץ ווודא שהבדיקות עוברות**

Run: `python test_competitors_intraday.py`
Expected: `12/12 passed`

- [ ] **Step 5: Commit**

```bash
git add competitors_intraday.py test_competitors_intraday.py
git commit -m "competitors_intraday: refresh rival posts and log gap candidates, never touching Kan tabs"
```

---

### Task 3: ה־workflow והטיימר

**Files:**
- Create: `.github/workflows/competitors_intraday.yml`
- Create: `social_dashboard/deploy/kan-competitors-intraday.sh`, `.service`, `.timer`

- [ ] **Step 1: ה־workflow**

```yaml
name: Competitors Intraday

on:
  workflow_dispatch:      # הטריגר: טיימר ב-VPS (kan-competitors-intraday.timer), כמו הסניפר.
                          # בכוונה בלי schedule: - ה-cron של GitHub מפגר בשעות.
    inputs:
      dry_run:
        description: 'מצב בדיקה: קורא, מחשב ומדפיס - בלי לכתוב לגיליון'
        required: false
        default: ''

permissions:
  contents: read
  actions: read

concurrency:
  group: competitors-intraday
  cancel-in-progress: false

jobs:
  refresh:
    runs-on: ubuntu-latest
    steps:
      # הריצה היומית כותבת את "פוסטים מתחרים" באותה שיטה (קריאה-מיזוג-כתיבה);
      # שתי ריצות במקביל = אחת דורסת את השנייה. אז מחכים לה לתור הבא.
      - name: Skip while the daily run is in flight
        id: guard
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          busy=$(gh run list --repo "$GITHUB_REPOSITORY" --workflow daily_update.yml --limit 5 \
            --json status --jq '[.[] | select(.status != "completed")] | length')
          echo "daily runs in flight: $busy"
          if [ "$busy" -gt 0 ]; then echo "skip=1" >> "$GITHUB_OUTPUT"; else echo "skip=0" >> "$GITHUB_OUTPUT"; fi

      - name: Checkout code
        if: steps.guard.outputs.skip == '0'
        uses: actions/checkout@v3

      - name: Set up Python
        if: steps.guard.outputs.skip == '0'
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        if: steps.guard.outputs.skip == '0'
        run: pip install -r requirements.txt

      - name: Refresh competitor posts and gap candidates
        if: steps.guard.outputs.skip == '0'
        env:
          FACEBOOK_TOKEN: ${{ secrets.FACEBOOK_TOKEN }}
          FACEBOOK_PAGE_ID: ${{ secrets.FACEBOOK_PAGE_ID }}
          GCP_SERVICE_ACCOUNT: ${{ secrets.GCP_SERVICE_ACCOUNT }}
          DRY_RUN: ${{ inputs.dry_run }}
        run: python competitors_intraday.py
```

- [ ] **Step 2: קבצי השרת**

`social_dashboard/deploy/kan-competitors-intraday.sh`:

```bash
#!/usr/bin/env bash
# Trigger the Competitors Intraday workflow at reliable local times (GitHub's
# scheduled-event queue fires hours late; same pattern as kan-hot-sniffer).
# Token from /etc/kan-dispatch.env.
set -euo pipefail
: "${GH_DISPATCH_TOKEN:?missing GH_DISPATCH_TOKEN}"

REPO="benzco89/kan-dashboard-auto"
WORKFLOW="competitors_intraday.yml"

code=$(curl -s -o /tmp/kan-competitors-intraday.out -w '%{http_code}' -X POST \
  -H "Authorization: Bearer ${GH_DISPATCH_TOKEN}" \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches" \
  -d '{"ref":"main"}')

if [ "$code" = "204" ]; then
  echo "$(date -Is) dispatched ${WORKFLOW} on main (HTTP 204)"
else
  echo "$(date -Is) dispatch FAILED (HTTP ${code}): $(cat /tmp/kan-competitors-intraday.out)"
  exit 1
fi
```

`social_dashboard/deploy/kan-competitors-intraday.service`:

```ini
[Unit]
Description=Trigger the Competitors Intraday GitHub workflow (rival posts + gap candidates)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=/etc/kan-dispatch.env
ExecStart=/usr/local/bin/kan-competitors-intraday
```

`social_dashboard/deploy/kan-competitors-intraday.timer`:

```ini
[Unit]
Description=Refresh competitor posts every 3h during the day (morning is the 08:30 daily run)

[Timer]
# five minutes after the sniffer, so the two do not hit the Graph API in the same second
OnCalendar=*-*-* 11,14,17,20,23:05:00 Asia/Jerusalem
Persistent=true
AccuracySec=5min

[Install]
WantedBy=timers.target
```

- [ ] **Step 3: בדיקה**

Run: `python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/competitors_intraday.yml',encoding='utf-8')); print('yaml ok')"` (אם `yaml` אינו מותקן: `pip show pyyaml` ודלג, וכתוב זאת בדוח). ואחרי זה `bash -n social_dashboard/deploy/kan-competitors-intraday.sh`.
Expected: `yaml ok`, ובלי שגיאת תחביר.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/competitors_intraday.yml social_dashboard/deploy/kan-competitors-intraday.sh social_dashboard/deploy/kan-competitors-intraday.service social_dashboard/deploy/kan-competitors-intraday.timer
git commit -m "competitors_intraday: workflow with a daily-run guard, and its VPS timer"
```

---

### Task 4: הדשבורד קורא את היומן

**Files:**
- Modify: `social_dashboard/gsheets.py` (`LAZY_SHEETS`)
- Modify: `social_dashboard/server.py` (`_PAGE_SHEETS["competitors"]`)
- Modify: `social_dashboard/templates/competitors.html` (`sourcesLine`)

- [ ] **Step 1: מימוש**

ב־`gsheets.py`, ב־`LAZY_SHEETS`, הוסף אחרי `"competitor_posts"`:

```python
    "gap_candidates": "מועמדי פערים",
```

ב־`server.py`, הוסף `"gap_candidates"` לסוף הרשומה של `_PAGE_SHEETS["competitors"]`.

ב־`competitors.html`, בפונקציה `sourcesLine`, החלף את השורה `parts.push("בדיקת הסיקור: IG, FB, YouTube, X, TikTok — נכון לאיסוף הבוקר");` ב:

```javascript
    if (f && f.posts_pulled_at && f.posts_pulled_at > f.pulled_at) {
      parts.push("ספירות הפוסטים: " + KS.esc(f.posts_pulled_at.slice(11, 16)));
    }
    parts.push(g.source === "intraday"
      ? "בדיקת הסיקור: IG ו־FB נכון ל־" + KS.esc(String(g.run_at).slice(11, 16)) + ", YouTube/X/TikTok נכון לבוקר"
      : "בדיקת הסיקור: IG, FB, YouTube, X, TikTok — נכון לאיסוף הבוקר");
```

- [ ] **Step 2: בדיקה**

Run (מתוך `social_dashboard/`): `for t in test_*.py; do venv/Scripts/python.exe $t | grep passed; done` — כולם עוברים.
הרץ את השרת (`venv/Scripts/python.exe -m uvicorn server:app --port 8433`, קריאה בלבד מהגיליון) ובדוק:
- `curl -s "localhost:8433/api/competitors?days=7"` מחזיר `gaps.source == "morning"`, כי הלשונית עוד לא קיימת. הדף לא נשבר.
- `freshness.posts_pulled_at` קיים.

אחר כך עצור את השרת.

- [ ] **Step 3: Commit**

```bash
git add social_dashboard/gsheets.py social_dashboard/server.py social_dashboard/templates/competitors.html
git commit -m "competitors page: read the intraday gap log, and say how fresh each part is"
```

---

### Task 5: מסמכים

**Files:**
- Modify: `docs/superpowers/specs/2026-09-27-competitors-design.md` (סעיף 2.3)
- Modify: `docs/ROADMAP.md` (פריט 19)

- [ ] **Step 1: המפרט**

בסעיף 2.3, בטבלת "מועמדי פערים", החלף את השורה של `outlets` ב:

```markdown
| `outlets` | JSON: רשימת `{username, post_id, eng, url}` — כל הפוסטים באשכול |
```

ומתחת לטבלה הוסף:

```markdown
כל ריצה כותבת גם שורת `kind = "run"` אחת, גם כשאין מועמדים. העמוד מתייחס לריצה כקיימת רק אם יש לה שורה כזו. בלעדיה, אחרי ריצה בלי מועמדים העמוד היה חוזר לחישוב מנתוני הבוקר, שבו ספירות המתחרים כבר מ־14:00 והפוסטים שלנו עדיין מהבוקר.
```

- [ ] **Step 2: ROADMAP**

בפריט 19, החלף את השורה שמתחילה ב־`- **2** — intraday run` ובשורה שאחריה ב:

```markdown
- **2** — intraday run built (`competitors_intraday.py`, `competitors_intraday.yml`,
  VPS timer `kan-competitors-intraday` at 11/14/17/20/23:05). Writes only the
  posts tab and two new tabs (post history, gap candidates). The daily run is
  untouched. Exit: 3 days of clean runs + `verify_collector check competitor_posts`.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-09-27-competitors-design.md docs/ROADMAP.md
git commit -m "docs: competitors stage 2 — run marker, JSON outlets, roadmap"
```

---

## אחרי המיזוג — הפעלה (הבקר בלבד, לא סוכן)

הסדר חשוב. אי אפשר להפעיל workflow מענף, ולכן הבדיקות החיות באות אחרי המיזוג. כל עוד הטיימר לא מותקן, אף אחד לא מפעיל את ה־workflow.

1. `gh workflow run competitors_intraday.yml --ref main -f dry_run=1`. בודקים בלוג: 15 חשבונות, פוסטים של כאן מ־IG ומ־FB, מועמדים, ו־"nothing written".
2. `python verify_collector.py snap competitor_posts`.
3. `gh workflow run competitors_intraday.yml --ref main` (ריצה חיה אחת).
4. `python verify_collector.py check competitor_posts`, ובדיקה ששתי הלשוניות החדשות נוצרו עם הכותרות הנכונות.
5. בעמוד החי: הקטע "סיפורים שאין לנו" מסומן "IG ו־FB נכון ל־HH:MM".
6. התקנת הטיימר בשרת: העתקת ה־sh ל־`/usr/local/bin/kan-competitors-intraday` (chmod +x), ה־service וה־timer ל־`/etc/systemd/system/`, ואז `systemctl daemon-reload && systemctl enable --now kan-competitors-intraday.timer`. לסיום, `systemctl list-timers | grep competitors`.
