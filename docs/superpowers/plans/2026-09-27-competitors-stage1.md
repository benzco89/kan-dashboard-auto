# עמוד המתחרים — שלב 1 ו־1ב: תוכנית ביצוע

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** כל מספר בעמוד המתחרים מחושב לפי הגדרה מפורשת שנבדקה, מקור שלא נטען לא מוצג כ"אין פערים", וארבעה חשבונות חדשים נכנסים למעקב.

**Architecture:** חישובי העמוד עוברים מ־`aggregate.py` למודול ממוקד חדש, `social_dashboard/competitors_page.py`, שכולו פונקציות טהורות שמקבלות `today`/`now` מבחוץ — כך אפשר לבדוק אותן על fixtures. `gsheets.py` מתחיל לרשום לכל לשונית אם הקריאה הצליחה, והעמוד בלבד משתמש בזה. העמוד מקבל בורר טווח משלו. האספן היומי משתנה בשורה אחת בלבד: רשימת החשבונות.

**Tech Stack:** Python 3.10, FastAPI, Google Sheets API (קריאה בלבד), HTML ו־JavaScript בלי build, CSS.

**Spec:** `docs/superpowers/specs/2026-09-27-competitors-design.md`. תוכנית זו מממשת את שלב 1 (סעיפים 3.1, 3.3 על נתוני הבוקר, 3.4, 3.5 בלי היסטוריה, 3.6, 4 בלי "עכשיו") ואת שלב 1ב (סעיף 1). שלבים 2–4 יקבלו תוכנית נפרדת.

## Global Constraints

- ממשק בעברית, RTL. כל מספר עם סימן (+/−) עטוף ב־`<bdi dir="ltr">`, אחרת ה־bidi הופך את הסימן.
- אין הרצה של אספן ייצור מקומית. האספן היומי רץ רק מהפייפליין.
- אין כתיבה ל־Google Sheets מהדשבורד. ה־scope שלו `spreadsheets.readonly`.
- אין ספריות חדשות ואין build step.
- הבדיקות בסגנון הקיים: קובץ Python שמריצים ישירות, עם `check(name, got, want)`, ויוצא בקוד 1 אם משהו נכשל.
- הרצת בדיקות: `venv/Scripts/python.exe <file>` מתוך `social_dashboard/`.
- ה־branch: `competitors-redesign`. לא מתחייבים על main.
- הטקסטים הבאים לא יופיעו בעמוד: "כל מה שרץ חזק אצל המתחרים קיבל כיסוי אצלנו", "שינויי עוקבים מצטברים מיום תחילת המעקב", "האיסוף התחיל היום".

## מפת קבצים

| קובץ | שינוי |
|---|---|
| `social_dashboard/gsheets.py` | רישום מצב קריאה לכל לשונית: `ok` / `stale` / `unavailable` |
| `social_dashboard/test_cache_warmer.py` | בדיקות למצב הקריאה |
| `social_dashboard/competitors_page.py` — **חדש** | כל חישובי העמוד |
| `social_dashboard/test_competitors.py` — **חדש** | בדיקות לחישובים |
| `social_dashboard/aggregate.py` | מחיקת `_coverage_gaps` ו־`build_competitors` |
| `social_dashboard/server.py` | חיבור ה־builder החדש, הוספת tiktok ללשוניות העמוד, `no-cache` לעמוד |
| `social_dashboard/static/app.js` | בורר טווח מקומי לעמוד אחד, הסבר לכפתור הרענון |
| `social_dashboard/static/app.css` | סגנונות בתחולת העמוד |
| `social_dashboard/templates/competitors.html` | כתיבה מחדש של הרינדור |
| `competitors_collector.py` | ארבעה חשבונות נוספים ברשימה |
| `docs/ROADMAP.md` | פריט פתוח לשלבים 2–4 |

---

### Task 1: מצב קריאה לכל לשונית ב־gsheets

**Files:**
- Modify: `social_dashboard/gsheets.py` (`_fetch`, `_load`, `_warm_once`, `SheetData`)
- Test: `social_dashboard/test_cache_warmer.py`

**Interfaces:**
- Produces: `gsheets.source_status(keys) -> dict[str, str]`, עם הערכים `"ok"`, `"stale"` או `"unavailable"`. מפתח שלא נקרא מעולם מחזיר `"ok"`. בנוסף, `SheetData.source_status() -> dict`, עם אותם ערכים, לכל הלשוניות שבאובייקט.
- Produces: מעכשיו `_fetch(keys)` מחזיר `None` (ולא `[]`) עבור לשונית שהקריאה שלה נכשלה.

- [ ] **Step 1: כתוב את הבדיקות שנכשלות**

ב־`test_cache_warmer.py`, מיד אחרי השורה `import gsheets  # noqa: E402`, הוסף:

```python
_real_fetch = gsheets._fetch
```

לפני השורה `print("-" * 62)` שבסוף הקובץ, הוסף:

```python
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
```

- [ ] **Step 2: הרץ ווודא שהבדיקות נכשלות**

Run: `venv/Scripts/python.exe test_cache_warmer.py`
Expected: `AttributeError: module 'gsheets' has no attribute '_status'`

- [ ] **Step 3: מימוש**

ב־`gsheets.py`, מתחת ל־`_cache = {}`:

```python
# outcome of the last read per tab: "ok" | "stale" (read failed, previous rows
# served) | "unavailable" (read failed, nothing to serve). A failed read used to
# look exactly like an empty sheet, and the competitors page said "no gaps".
_status = {}
```

ב־`_fetch`, בלולאת ה־fallback, שנה את `out[k] = []` ל:

```python
        except Exception:
            out[k] = None       # failed — not "empty"; _store decides what to serve
```

והוסף מעל `_fresh`:

```python
def _store(key, rows, stamp):
    """Caller holds _lock. rows=None means the read failed."""
    if rows is None:
        if key in _cache and _cache[key][0]:
            _status[key] = "stale"            # keep the rows and their old stamp
        else:
            _cache[key] = ([], 0)             # stamp 0: retried on the next request
            _status[key] = "unavailable"
        return
    _cache[key] = (rows, stamp)
    _status[key] = "ok"


def source_status(keys):
    with _lock:
        return {k: _status.get(k, "ok") for k in keys}
```

ב־`_load`, החלף את `_cache[k] = (rows, stamp)` ב־`_store(k, rows, stamp)`.

ב־`_warm_once`, החלף את גוף הלולאה:

```python
        for k, rows in fetched.items():
            # a failed read returns None (or [] from an older fake); keeping the
            # previous rows is better than serving an empty dashboard
            if rows is None:
                _store(k, None, stamp)
            elif rows or not _cache.get(k, ([], 0))[0]:
                _store(k, rows, stamp)
```

ב־`SheetData`, הוסף מתודה:

```python
    def source_status(self):
        """Whether each tab in here came from a good read. Pages that must not
        mistake a failure for "nothing found" ask; the rest ignore it."""
        return source_status(list(self.keys()))
```

- [ ] **Step 4: הרץ ווודא שהבדיקות עוברות**

Run: `venv/Scripts/python.exe test_cache_warmer.py`
Expected: השורה האחרונה `N/N passed`, בלי `FAIL`

- [ ] **Step 5: Commit**

```bash
git add social_dashboard/gsheets.py social_dashboard/test_cache_warmer.py
git commit -m "gsheets: a failed read is marked stale/unavailable, not served as an empty sheet"
```

---

### Task 2: השוואה לאורך זמן — זוג תאריכים אחד לכולם

**Files:**
- Create: `social_dashboard/competitors_page.py`
- Create: `social_dashboard/test_competitors.py`

**Interfaces:**
- Consumes: מ־`aggregate` — `_parse_date`, `_int`.
- Produces:
  - `snapshots_by_user(rows) -> dict[str, list[tuple[date, dict]]]` — ממוין לפי תאריך.
  - `comparison_window(by_user, days) -> dict | None` — המפתחות: `end`, `base`, `requested_base`, `history_start` (כולם `date`), `partial` (`bool`), `covered_days` (`int`).
  - `growth(entries, win, key="followers") -> dict` — המפתחות: `status` (`"ok"`, `"new"` או `"none"`), `change` (`int` או `None`), `pct` (`float` או `None`), `base_date` ו־`end_date` (`str` או `None`).
  - `kan_entries(followers_rows) -> list[tuple[date, dict]]` — הצורה של `entries`, עם `{"followers": int}`.

- [ ] **Step 1: כתוב את הבדיקות שנכשלות**

צור את `social_dashboard/test_competitors.py`:

```python
"""Locks the competitors page — every window on it, because every one broke.

Audit of 2026-09-27 against the live sheet: Kan's growth read 482 at 7, 14, 30
and 90 days (a weekly sum), the arena moved with the range picker, and every
"coverage gap" was one rival's own exclusive. Each block below replays one of
those against a fixture with a fixed today.

    python social_dashboard/test_competitors.py
"""

import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import competitors_page as C  # noqa: E402

PASS = FAIL = 0
TODAY = date(2026, 9, 27)
NOW = datetime(2026, 9, 27, 12, 0)


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


def days_between(a, b):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def snaps(user, first, step, start_value, last=TODAY):
    return [{"date": str(d), "username": user, "name": user.upper(),
             "followers": start_value + i * step, "followers_change": step,
             "eng_per_1k": 5, "pulled_at": f"{d} 08:40"}
            for i, d in enumerate(days_between(first, last))]


SNAPS = (snaps("aaa", date(2026, 7, 13), 10, 1000)
         + snaps("bbb", date(2026, 7, 13), 1, 5000)
         + snaps("ccc", date(2026, 9, 25), 5, 300))            # added two days ago
FOLLOWERS = [{"date": str(d), "ig_followers": 280000 + i * 50, "ig_followers_change": 50}
             for i, d in enumerate(days_between(date(2026, 6, 1), TODAY))]

print("\ncomparison window\n" + "-" * 62)
by = C.snapshots_by_user(SNAPS)
kan = C.kan_entries(FOLLOWERS)

w7 = C.comparison_window(by, 7)
check("7d: ends on the latest snapshot", w7["end"], TODAY)
check("7d: base is end minus 7", w7["base"], date(2026, 9, 20))
check("7d: not partial", w7["partial"], False)
check("7d: rival growth", C.growth(by["aaa"], w7)["change"], 70)
check("7d: Kan measured over the same pair", C.growth(kan, w7)["change"], 350)
check("7d: same base date for Kan and a rival",
      C.growth(kan, w7)["base_date"], C.growth(by["aaa"], w7)["base_date"])

w30 = C.comparison_window(by, 30)
check("30d: Kan growth follows the range (was stuck on the weekly figure)",
      C.growth(kan, w30)["change"], 1500)
check("30d: percent is against the base", C.growth(by["aaa"], w30)["pct"],
      round(300 / (1000 + 46 * 10) * 100, 2))

w90 = C.comparison_window(by, 90)
check("90d: history shorter than the range is partial", w90["partial"], True)
check("90d: base moves to the start of history", w90["base"], date(2026, 7, 13))
check("90d: covered days are reported", w90["covered_days"], 76)
check("90d: Kan uses the moved base too", C.growth(kan, w90)["base_date"], "2026-07-13")
check("90d: Kan change over 76 days", C.growth(kan, w90)["change"], 76 * 50)

check("a new account is not ranked on growth", C.growth(by["ccc"], w7)["status"], "new")
check("and has no change", C.growth(by["ccc"], w7)["change"], None)
check("an account without snapshots has no growth",
      C.growth([], w7)["status"], "none")
```

(בסוף הקובץ, מתחת לכל הבלוקים, תמיד:)

```python
print("-" * 62)
print(f"{PASS}/{PASS + FAIL} passed")
sys.exit(1 if FAIL else 0)
```

המשימות הבאות מוסיפות בלוקים **מעל** שלוש השורות האלה.

- [ ] **Step 2: הרץ ווודא שהבדיקות נכשלות**

Run: `venv/Scripts/python.exe test_competitors.py`
Expected: `ModuleNotFoundError: No module named 'competitors_page'`

- [ ] **Step 3: מימוש**

צור את `social_dashboard/competitors_page.py`:

```python
"""עמוד המתחרים — /api/competitors.

כל חלון זמן בעמוד מוגדר כאן במפורש, כי כל אחד מהם נשבר פעם בגלל הגדרה
משתמעת (ביקורת 2026-09-27, docs/superpowers/specs/2026-09-27-competitors-design.md):

  * השוואה לאורך זמן — זוג תאריכים אחד (בסיס, סוף) לכל החשבונות וגם לכאן.
    כאן הוצג 482 בכל טווח, כי חושב מסכום שבועי.
  * הזירה — אתמול ושלשום, קבוע. היא נבחרה פעם מתוך 15 החזקים בטווח, ולכן
    זזה עם הבורר.
  * פערים — 72 שעות לפי שעת הפרסום; חציון מפוסטים בשלים בלבד; סיפור של
    מתחרה אחד הוא "בלעדי", לא "פספוס".
  * מקור שלא נטען אינו "אין פערים".

הפונקציות מקבלות today/now מבחוץ כדי שאפשר יהיה לבדוק אותן.
"""

from datetime import datetime, timedelta

import aggregate as A

NEW_ACCOUNT_SLACK_DAYS = 1   # יום אחד בולע ריצה שנכשלה; יותר מזה = "חדש במעקב",
                             # אחרת חשבון בן 4 ימים נמדד מול 7 ימים של האחרים


# ---------- השוואה לאורך זמן ----------

def snapshots_by_user(rows):
    out = {}
    for r in rows:
        d = A._parse_date(r.get("date"))
        u = str(r.get("username", "")).strip()
        if d and u:
            out.setdefault(u, []).append((d, r))
    for entries in out.values():
        entries.sort(key=lambda e: e[0])
    return out


def comparison_window(by_user, days):
    """זוג התאריכים שכל החשבונות, וכאן, נמדדים ביניהם. כשההיסטוריה קצרה
    מהטווח, הבסיס של כל ההשוואה עובר לתחילתה ומסומן partial."""
    if not by_user:
        return None
    end = max(entries[-1][0] for entries in by_user.values())
    history_start = min(entries[0][0] for entries in by_user.values())
    requested = end - timedelta(days=days)
    base = max(requested, history_start)
    return {
        "end": end, "base": base, "requested_base": requested,
        "history_start": history_start,
        "partial": base > requested,
        "covered_days": (end - base).days,
    }


def growth(entries, win, key="followers"):
    """שינוי בין המדידה הראשונה מהבסיס והלאה לאחרונה עד הסוף."""
    none = {"status": "none", "change": None, "pct": None, "base_date": None, "end_date": None}
    if not win:
        return none
    end_e = next((e for e in reversed(entries) if e[0] <= win["end"]), None)
    base_e = next((e for e in entries if e[0] >= win["base"]), None)
    if not end_e or not base_e or base_e[0] > end_e[0]:
        return none
    if (base_e[0] - win["base"]).days > NEW_ACCOUNT_SLACK_DAYS:
        return {"status": "new", "change": None, "pct": None,
                "base_date": str(base_e[0]), "end_date": str(end_e[0])}
    first, last = A._int(base_e[1].get(key)), A._int(end_e[1].get(key))
    change = last - first
    return {"status": "ok", "change": change,
            "pct": round(change / first * 100, 2) if first else None,
            "base_date": str(base_e[0]), "end_date": str(end_e[0])}


def kan_entries(followers_rows):
    """מעקב עוקבים -> אותה צורה של צילומי המתחרים. תא ריק מדולג."""
    out = []
    for r in followers_rows:
        d, v = A._parse_date(r.get("date")), A._int(r.get("ig_followers"))
        if d and v:
            out.append((d, {"followers": v}))
    out.sort(key=lambda e: e[0])
    return out
```

- [ ] **Step 4: הרץ ווודא שהבדיקות עוברות**

Run: `venv/Scripts/python.exe test_competitors.py`
Expected: `16/16 passed`

- [ ] **Step 5: Commit**

```bash
git add social_dashboard/competitors_page.py social_dashboard/test_competitors.py
git commit -m "competitors: one date pair for every account and for Kan"
```

---

### Task 3: זירה בחלון קבוע ופוסטים ליום

**Files:**
- Modify: `social_dashboard/competitors_page.py`
- Test: `social_dashboard/test_competitors.py`

**Interfaces:**
- Consumes: `A._parse_date`, `A._int`, `A._clean_caption`.
- Produces:
  - `post_view(p, cap_col, username, name, is_kan) -> dict` — המפתחות: `username`, `name`, `is_kan`, `date`, `time`, `type`, `caption`, `likes`, `comments`, `eng`, `url`.
  - `arena(comp_posts, kan_posts, names, today) -> {"dates": [str, str], "posts": list[post_view]}`.
  - `feed_window(posts, days, today) -> {"first": date, "last": date, "days": int} | None`.
  - `posts_per_day(posts, fw) -> float | None`.

- [ ] **Step 1: כתוב את הבדיקות שנכשלות**

ב־`test_competitors.py`, מעל שורות הסיכום:

```python
print("\narena and posting rate\n" + "-" * 62)


def post(user, d, t, likes, comments=0, caption="", pulled=None, pid=None):
    return {"post_id": pid or f"{user}-{d}-{t}-{likes}", "username": user, "date": d, "time": t,
            "type": "VIDEO", "caption": caption, "likes": likes, "comments": comments,
            "permalink": f"https://instagram.com/p/{user}{d}{t}", "pulled_at": pulled or f"{d} 23:00"}


# 16 strong posts ten days old and one weak post from yesterday: the weak one
# must still reach the arena (it used to be cut by a per-account top-15 first)
ARENA_POSTS = ([post("aaa", "2026-09-17", f"{h:02d}:00", 5000) for h in range(16)]
               + [post("bbb", "2026-09-26", "10:00", 10)]
               + [post("aaa", "2026-09-27", "07:00", 9000)])           # today: outside
KAN_IG = [{"media_id": "k1", "date": "2026-09-25", "time": "09:00", "type": "Reel",
           "caption": "כאן", "likes": 100, "comments": 5, "permalink": "https://instagram.com/p/k1"}]
NAMES = {"aaa": "AAA", "bbb": "BBB"}

a = C.arena(ARENA_POSTS, KAN_IG, NAMES, TODAY)
check("arena window is the day before yesterday and yesterday", a["dates"], ["2026-09-25", "2026-09-26"])
check("a weak fresh post still makes the arena", [p["username"] for p in a["posts"]], ["kan_news", "bbb"])
check("Kan is marked", a["posts"][0]["is_kan"], True)
check("today's posts are outside the window", any(p["date"] == "2026-09-27" for p in a["posts"]), False)

FEED = [post("aaa", str(d), "10:00", 1) for d in days_between(date(2026, 9, 13), TODAY) for _ in (0, 1)]
fw7 = C.feed_window(FEED, 7, TODAY)
check("7d feed window: last seven full days", (str(fw7["first"]), str(fw7["last"]), fw7["days"]),
      ("2026-09-20", "2026-09-26", 7))
check("posts per day is an average over the window", C.posts_per_day(FEED, fw7), 2.0)
fw30 = C.feed_window(FEED, 30, TODAY)
check("30d is clipped to what the feed keeps", fw30["days"], 14)
check("no feed, no rate", C.posts_per_day(FEED, C.feed_window([], 7, TODAY)), None)
```

- [ ] **Step 2: הרץ ווודא שהבדיקות נכשלות**

Run: `venv/Scripts/python.exe test_competitors.py`
Expected: `AttributeError: module 'competitors_page' has no attribute 'arena'`

- [ ] **Step 3: מימוש**

ב־`competitors_page.py`, מתחת לקבוע `NEW_ACCOUNT_SLACK_DAYS`:

```python
ARENA_TOP = 12
```

ובסוף הקובץ:

```python
# ---------- פוסטים ----------

def post_view(p, cap_col, username, name, is_kan):
    likes, comments = A._int(p.get("likes")), A._int(p.get("comments"))
    return {"username": username, "name": name, "is_kan": is_kan,
            "date": str(A._parse_date(p.get("date")) or ""),
            "time": str(p.get("time", ""))[:5],
            "type": p.get("type", ""),
            "caption": A._clean_caption(p.get(cap_col, ""))[:200],
            "likes": likes, "comments": comments, "eng": likes + comments,
            "url": p.get("permalink", "")}


def arena(comp_posts, kan_posts, names, today):
    """שלשום ואתמול, מכל הפוסטים בחלון — סינון לפני מיון וחיתוך."""
    days = (today - timedelta(days=2), today - timedelta(days=1))
    items = []
    for p in comp_posts:
        if A._parse_date(p.get("date")) in days:
            u = str(p.get("username", "")).strip()
            items.append(post_view(p, "caption", u, names.get(u, u), False))
    for p in kan_posts:
        if A._parse_date(p.get("date")) in days:
            items.append(post_view(p, "caption", "kan_news", "כאן חדשות", True))
    items.sort(key=lambda x: -x["eng"])
    return {"dates": [str(days[0]), str(days[1])], "posts": items[:ARENA_TOP]}


def feed_window(posts, days, today):
    """הימים המלאים שהפיד השמור (עד 14 יום) מכסה בתוך הטווח: [first, אתמול]."""
    last = today - timedelta(days=1)
    dates = [d for d in (A._parse_date(p.get("date")) for p in posts) if d]
    if not dates:
        return None
    first = max(last - timedelta(days=days - 1), min(dates))
    if first > last:
        return None
    return {"first": first, "last": last, "days": (last - first).days + 1}


def _in_window(p, fw):
    d = A._parse_date(p.get("date"))
    return d is not None and fw["first"] <= d <= fw["last"]


def posts_per_day(posts, fw):
    if not fw:
        return None
    return round(sum(1 for p in posts if _in_window(p, fw)) / fw["days"], 1)
```

- [ ] **Step 4: הרץ ווודא שהבדיקות עוברות**

Run: `venv/Scripts/python.exe test_competitors.py`
Expected: `24/24 passed`

- [ ] **Step 5: Commit**

```bash
git add social_dashboard/competitors_page.py social_dashboard/test_competitors.py
git commit -m "competitors: arena on a fixed two-day window, posts/day as a real average"
```

---

### Task 4: פערים — פספוס מול בלעדי, חציון בשל, מקור חסר

**Files:**
- Modify: `social_dashboard/competitors_page.py`
- Test: `social_dashboard/test_competitors.py`

**Interfaces:**
- Consumes: `A._parse_date`, `A._int`, `A._median`, `A._viral_tokens`, `A._clean_caption`, `A._MIN_TOKENS`, `A._MATCH_CONTAINMENT`.
- Produces:
  - `account_medians(comp_posts) -> dict[str, float]`.
  - `coverage_gaps(data, now, sources=None) -> dict` — המפתחות:
    - `status`: `"ok"`, `"stale"` או `"unavailable"`
    - `failed`, `stale`: רשימות של מפתחות לשוניות
    - `checked`: `int`
    - `missed`, `exclusive`: רשימות של gap

    כל gap: `caption`, `date`, `time`, `n_outlets`, `total_eng`, `lead` (`username`, `eng`, `median`, `ratio`, `threshold`), `posts` (רשימה של `username`, `eng`, `url`).

- [ ] **Step 1: כתוב את הבדיקות שנכשלות**

ב־`test_competitors.py`, מעל שורות הסיכום:

```python
print("\ncoverage gaps\n" + "-" * 62)

MATURE = ([post("aaa", "2026-09-20", "10:00", 200, pulled="2026-09-24 08:40", pid=f"am{i}") for i in range(10)]
          + [post("bbb", "2026-09-20", "10:00", 100, pulled="2026-09-24 08:40", pid=f"bm{i}") for i in range(10)])
YOUNG = [post("aaa", "2026-09-27", "06:00", 10, pulled="2026-09-27 08:40", pid="ay")]
STORY = "הפגנה גדולה בכיכר הבימה נגד הממשלה אלפי מפגינים הגיעו"
EXCL = "ראיון בלעדי הזמרת המפורסמת מדברת על הקריירה והמשפחה שלה"
QUAKE = "רעידת אדמה חזקה הורגשה בצפון הארץ בלילה ללא נפגעים"
GAP_POSTS = MATURE + YOUNG + [
    post("aaa", "2026-09-26", "09:00", 2000, caption=STORY, pid="x1"),
    post("bbb", "2026-09-26", "11:00", 900, caption=STORY + " היום", pid="x2"),
    post("aaa", "2026-09-26", "13:00", 1500, caption=EXCL, pid="y1"),
    post("aaa", "2026-09-26", "18:00", 1200, caption=EXCL + " בערב", pid="y2"),
    post("aaa", "2026-09-26", "20:00", 3000, caption=QUAKE, pid="z1"),
    post("aaa", "2026-09-23", "10:00", 5000, caption="סיפור ישן מאוד שכבר לא בחלון הזמן", pid="old"),
    post("ddd", "2026-09-26", "10:00", 400, caption="חשבון בלי פוסטים בשלים מפרסם כתבה על מזג האוויר", pid="d1"),
]
GAP_DATA = {"competitor_posts": GAP_POSTS,
            "instagram": [{"date": "2026-09-26", "caption": "רעידת אדמה חזקה הורגשה בצפון הארץ הלילה תושבים דיווחו"}],
            "facebook": [], "youtube": [], "twitter": [], "tiktok": []}

med = C.account_medians(GAP_POSTS)
check("median comes from mature posts only", med["aaa"], 200)
check("an account with no mature post has no median", "ddd" in med, False)

g = C.coverage_gaps(GAP_DATA, NOW)
check("status ok when every source loaded", g["status"], "ok")
check("a story at two rivals is 'missed'", [x["n_outlets"] for x in g["missed"]], [2])
check("its explanation is against the lead's own median", g["missed"][0]["lead"]["ratio"], 10.0)
excl = {x["caption"][:10]: x for x in g["exclusive"]}
check("one rival twice is still one outlet", excl[EXCL[:10]]["n_outlets"], 1)
check("and keeps both links", len(excl[EXCL[:10]]["posts"]), 2)
check("a story Kan posted is not a gap",
      any(QUAKE[:10] in x["caption"] for x in g["missed"] + g["exclusive"]), False)
check("posts older than 72h are out",
      any("ישן" in x["caption"] for x in g["missed"] + g["exclusive"]), False)
zero = [x for x in g["exclusive"] if x["lead"]["username"] == "ddd"][0]
check("no median: no ratio", zero["lead"]["ratio"], None)
check("no median: the floor is the threshold", zero["lead"]["threshold"], 300)

u = C.coverage_gaps(GAP_DATA, NOW, {"instagram": "unavailable"})
check("a source that did not load: unavailable", u["status"], "unavailable")
check("and names it", u["failed"], ["instagram"])
check("and claims nothing", (u["missed"], u["exclusive"]), ([], []))
s = C.coverage_gaps(GAP_DATA, NOW, {"facebook": "stale"})
check("a stale source still shows gaps, flagged", (s["status"], len(s["missed"])), ("stale", 1))
```

- [ ] **Step 2: הרץ ווודא שהבדיקות נכשלות**

Run: `venv/Scripts/python.exe test_competitors.py`
Expected: `AttributeError: module 'competitors_page' has no attribute 'account_medians'`

- [ ] **Step 3: מימוש**

ב־`competitors_page.py`, מתחת ל־`ARENA_TOP`:

```python
GAPS_TOP = 10
GAP_WINDOW_H = 72         # פוסט מתחרה נבדק 72 שעות מפרסומו
OURS_WINDOW_H = 96        # מול כל מה שפרסמנו ב־96 השעות האחרונות
MATURE_H = 48             # חציון החשבון רק מפוסטים שהבשילו
GAP_FLOOR = 300
GAP_MULT = 3
# בלי כל אחד מאלה, כל פוסט מתחרה נראה כפער
GAP_SOURCES = ("competitor_posts", "instagram", "facebook", "youtube", "twitter", "tiktok")
# (sheet key, caption column, date column)
OUR_SOURCES = (("instagram", "caption", "date"), ("facebook", "title", "date"),
               ("youtube", "title", "published_at"), ("twitter", "text", "date"),
               ("tiktok", "title", "date"))
```

ובסוף הקובץ:

```python
# ---------- פערים ----------

def _ts(date_s, time_s=""):
    d = str(date_s or "").strip()[:10]
    t = str(time_s or "").strip()[:5] or "00:00"
    try:
        return datetime.strptime(d + " " + t, "%Y-%m-%d %H:%M")
    except ValueError:
        return None


def _eng(p):
    return A._int(p.get("likes")) + A._int(p.get("comments"))


def account_medians(comp_posts):
    """חציון לייקים+תגובות לחשבון, רק מפוסטים שהיו בני 48 שעות ומעלה במשיכה
    האחרונה שלהם. פוסט צעיר מוריד את החציון ומנפח כל מכפיל."""
    per = {}
    for p in comp_posts:
        posted = _ts(p.get("date"), p.get("time"))
        pulled_s = str(p.get("pulled_at", ""))
        pulled = _ts(pulled_s[:10], pulled_s[11:16])
        if posted and pulled and pulled - posted >= timedelta(hours=MATURE_H):
            per.setdefault(str(p.get("username", "")).strip(), []).append(_eng(p))
    return {u: A._median(v) for u, v in per.items()}


def our_token_sets(data, now):
    since = (now - timedelta(hours=OURS_WINDOW_H)).date()
    out = []
    for key, cap_col, date_col in OUR_SOURCES:
        for p in data.get(key, []) or []:
            d = A._parse_date(p.get(date_col))
            if d and d >= since:
                toks = A._viral_tokens(p.get(cap_col, ""))
                if len(toks) >= A._MIN_TOKENS:
                    out.append(toks)
    return out


def _covered(toks, ours):
    # 0.3 ו־4 מילים, לא 0.6: מתחרה מנסח את אותו סיפור במילים שלו
    # (גלית/תאילנד חפפה 0.35 בין N12 לכאן וסומנה בטעות כפער ב־0.6)
    for o in ours:
        inter = len(toks & o)
        if inter >= 4 and inter / min(len(toks), len(o)) >= 0.3:
            return True
    return False


def coverage_gaps(data, now, sources=None):
    sources = sources or {}
    unavailable = [k for k in GAP_SOURCES if sources.get(k, "ok") == "unavailable"]
    stale = [k for k in GAP_SOURCES if sources.get(k, "ok") == "stale"]
    if unavailable:
        return {"status": "unavailable", "failed": unavailable, "stale": stale,
                "checked": 0, "missed": [], "exclusive": []}

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
        cands.append({"username": u, "posted": posted, "eng": eng, "median": m,
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

    missed, exclusive = [], []
    for g in clusters:
        lead = g["posts"][0]
        users = list(dict.fromkeys(c["username"] for c in g["posts"]))
        item = {
            "caption": lead["caption"],
            "date": lead["posted"].strftime("%Y-%m-%d"),
            "time": lead["posted"].strftime("%H:%M"),
            "n_outlets": len(users),
            "total_eng": sum(c["eng"] for c in g["posts"]),
            "lead": {k: lead[k] for k in ("username", "eng", "median", "ratio", "threshold")},
            "posts": [{"username": c["username"], "eng": c["eng"], "url": c["url"]}
                      for c in g["posts"]],
        }
        (missed if len(users) >= 2 else exclusive).append(item)

    return {"status": "stale" if stale else "ok", "failed": [], "stale": stale,
            "checked": len(cands), "missed": missed[:GAPS_TOP], "exclusive": exclusive[:GAPS_TOP]}
```

- [ ] **Step 4: הרץ ווודא שהבדיקות עוברות**

Run: `venv/Scripts/python.exe test_competitors.py`
Expected: `39/39 passed`

- [ ] **Step 5: Commit**

```bash
git add social_dashboard/competitors_page.py social_dashboard/test_competitors.py
git commit -m "competitors: split gaps into missed (2+ rivals) and exclusive; no verdict without sources"
```

---

### Task 5: `build()`, חיבור לשרת ומחיקת הקוד הישן

**Files:**
- Modify: `social_dashboard/competitors_page.py`
- Modify: `social_dashboard/server.py:39-72`
- Modify: `social_dashboard/aggregate.py` — מחיקת הבלוק מ־`# ---------- competitors (IG business discovery snapshots) ----------` ועד השורה שלפני `# ---------- alerts / anomaly detection ----------`
- Test: `social_dashboard/test_competitors.py`

**Interfaces:**
- Consumes: כל מה ש־Task 2–4 מייצרות, `A.israel_today`, `A._TZ`, `A._last_data_date`, `A._num`.
- Produces: `build(data, days, today=None, now=None) -> dict` — המפתחות: `range`, `last_date`, `window`, `feed`, `freshness`, `sources`, `summary` (`kan_rank`, `ranked`, `kan_growth`), `arena`, `gaps`, `competitors`.

  כל שורה ב־`competitors` כוללת: `username`, `name`, `is_kan`, `followers`, `as_of`, `change_1d`, `growth`, `posts_per_day`, `eng_per_1k`, `spark`, `posts`.
- Produces: `freshness(by_user) -> {"date", "pulled_at", "updated", "known", "missing"} | None`.

- [ ] **Step 1: כתוב את הבדיקות שנכשלות**

ב־`test_competitors.py`, מעל שורות הסיכום:

```python
print("\nbuild\n" + "-" * 62)
DATA = dict(GAP_DATA, competitors=SNAPS, followers=FOLLOWERS,
            competitor_posts=GAP_POSTS + ARENA_POSTS, instagram=KAN_IG + GAP_DATA["instagram"])
b7 = C.build(DATA, 7, today=TODAY, now=NOW)
b90 = C.build(DATA, 90, today=TODAY, now=NOW)
check("the arena does not move with the range", b7["arena"], b90["arena"])
check("the gaps do not move with the range", b7["gaps"], b90["gaps"])
kan7 = [c for c in b7["competitors"] if c["is_kan"]][0]
check("Kan's row carries the range growth", kan7["growth"]["change"], 350)
check("Kan's rank counts every account", (b7["summary"]["kan_rank"], b7["summary"]["ranked"]), (1, 4))
check("the partial flag reaches the payload", b90["window"]["partial"], True)
check("dates are strings in the payload", b90["window"]["base"], "2026-07-13")
check("freshness counts today's snapshots", (b7["freshness"]["updated"], b7["freshness"]["known"]), (3, 3))
check("a plain dict has no source status", b7["sources"], {})

stale_snaps = SNAPS + [{"date": "2026-09-26", "username": "eee", "name": "E", "followers": 10}]
f = C.freshness(C.snapshots_by_user(stale_snaps))
check("an account that missed today's run is named", f["missing"], ["eee"])
```

- [ ] **Step 2: הרץ ווודא שהבדיקות נכשלות**

Run: `venv/Scripts/python.exe test_competitors.py`
Expected: `AttributeError: module 'competitors_page' has no attribute 'build'`

- [ ] **Step 3: מימוש ב־`competitors_page.py`**

מתחת ל־`GAP_MULT`:

```python
POSTS_PER_ACCOUNT = 15
FEED_KEEP_DAYS = 14       # כמו POSTS_RETENTION_DAYS באספן
```

ובסוף הקובץ:

```python
# ---------- העמוד ----------

def freshness(by_user):
    if not by_user:
        return None
    last = max(e[-1][0] for e in by_user.values())
    known = [u for u, e in by_user.items() if (last - e[-1][0]).days <= 7]
    today_rows = [e[-1][1] for e in by_user.values() if e[-1][0] == last]
    return {
        "date": str(last),
        "pulled_at": max((str(r.get("pulled_at", "")) for r in today_rows), default=""),
        "updated": len(today_rows),
        "known": len(known),
        "missing": sorted(u for u in known if by_user[u][-1][0] != last),
    }


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


def build(data, days, today=None, now=None):
    today = today or A.israel_today()
    if now is None:
        now = datetime.now(A._TZ).replace(tzinfo=None) if A._TZ else datetime.now()
    sources = data.source_status() if hasattr(data, "source_status") else {}

    by_user = snapshots_by_user(data.get("competitors", []) or [])
    comp_posts = data.get("competitor_posts", []) or []
    ig = data.get("instagram", []) or []
    followers = data.get("followers", []) or []
    win = comparison_window(by_user, days)
    fw = feed_window(comp_posts, days, today)

    posts_by_user = {}
    for p in comp_posts:
        posts_by_user.setdefault(str(p.get("username", "")).strip(), []).append(p)

    names, rows = {}, []
    for u, entries in by_user.items():
        latest = entries[-1][1]
        names[u] = latest.get("name") or u
        own = posts_by_user.get(u, [])
        rows.append({
            "username": u, "name": names[u], "is_kan": False,
            "followers": A._int(latest.get("followers")),
            "as_of": str(entries[-1][0]),
            "change_1d": A._int(latest.get("followers_change")),
            "growth": growth(entries, win),
            "posts_per_day": posts_per_day(own, fw),
            "eng_per_1k": round(A._num(latest.get("eng_per_1k")), 2),
            "spark": [A._int(r.get("followers")) for d, r in entries if win and d >= win["base"]],
            "posts": sorted((post_view(p, "caption", u, names[u], False) for p in own),
                            key=lambda x: -x["eng"])[:POSTS_PER_ACCOUNT],
        })

    # כאן — מהנתונים המלאים שלנו, באותם תאריכים בדיוק
    kan = kan_entries(followers)
    kan_followers = kan[-1][1]["followers"] if kan else 0
    recent = sorted((p for p in ig if A._parse_date(p.get("date"))),
                    key=lambda p: (str(p.get("date")), str(p.get("time", ""))), reverse=True)[:10]
    avg = sum(_eng(p) for p in recent) / len(recent) if recent else 0
    keep_from = today - timedelta(days=FEED_KEEP_DAYS)
    rows.append({
        "username": "kan_news", "name": "כאן חדשות", "is_kan": True,
        "followers": kan_followers,
        "as_of": str(kan[-1][0]) if kan else None,
        "change_1d": A._int(followers[-1].get("ig_followers_change")) if followers else 0,
        "growth": growth(kan, win),
        "posts_per_day": posts_per_day(ig, fw),
        "eng_per_1k": round(avg / kan_followers * 1000, 2) if kan_followers else 0,
        "spark": [r["followers"] for d, r in kan if win and d >= win["base"]],
        "posts": sorted((post_view(p, "caption", "kan_news", "כאן חדשות", True) for p in ig
                         if (A._parse_date(p.get("date")) or keep_from) > keep_from),
                        key=lambda x: -x["eng"])[:POSTS_PER_ACCOUNT],
    })

    rows.sort(key=lambda c: -c["followers"])
    kan_rank = next(i + 1 for i, c in enumerate(rows) if c["is_kan"])
    return {
        "range": days,
        "last_date": A._last_data_date(data),
        "window": {k: _iso(v) for k, v in win.items()} if win else None,
        "feed": {k: _iso(v) for k, v in fw.items()} if fw else None,
        "freshness": freshness(by_user),
        "sources": sources,
        "summary": {"kan_rank": kan_rank, "ranked": len(rows),
                    "kan_growth": rows[kan_rank - 1]["growth"]},
        "arena": arena(comp_posts, ig, names, today),
        "gaps": coverage_gaps(data, now, sources),
        "competitors": rows,
    }
```

- [ ] **Step 4: הרץ ווודא שהבדיקות עוברות**

Run: `venv/Scripts/python.exe test_competitors.py`
Expected: `48/48 passed`

- [ ] **Step 5: חבר לשרת ומחק את הקוד הישן**

ב־`server.py`:
- הוסף `import competitors_page` מתחת ל־`import aggregate`.
- ב־`_PAGE_SHEETS["competitors"]` הוסף `"tiktok"`: `("competitors", "competitor_posts", "facebook", "instagram", "youtube", "twitter", "tiktok", "followers"),`
- ב־`_BUILDERS` החלף את `aggregate.build_competitors` ב־`competitors_page.build`.
- ב־`revalidate_assets` הוסף `"/competitors"` ו־`"/viral"` לרשימת הנתיבים.

ב־`aggregate.py`, מחק את `_coverage_gaps` ואת `build_competitors`: כל השורות מהכותרת `# ---------- competitors (IG business discovery snapshots) ----------` ועד השורה שלפני `# ---------- alerts / anomaly detection ----------`.

- [ ] **Step 6: ודא שאין עוד מי שקורא לקוד שנמחק, ושכל הבדיקות עוברות**

Run (מתוך שורש הריפו): `grep -rn "build_competitors\|_coverage_gaps" --include=*.py . | grep -v venv`
Expected: אין פלט.

Run (מתוך `social_dashboard/`): `for t in test_*.py; do venv/Scripts/python.exe $t | tail -1; done`
Expected: כל שורה `N/N passed`.

- [ ] **Step 7: Commit**

```bash
git add social_dashboard/competitors_page.py social_dashboard/test_competitors.py social_dashboard/server.py social_dashboard/aggregate.py
git commit -m "competitors: serve the page from competitors_page.build; drop the old builder"
```

---

### Task 6: בורר טווח מקומי והסבר לכפתור הרענון

**Files:**
- Modify: `social_dashboard/static/app.js:290-400`

**Interfaces:**
- Produces:
  - `KanSocial.init(page, onRender, opts)`. `opts.rangeKey` (ברירת מחדל `"pm_range"`) קובע איפה נשמר הטווח. אם `opts.headerRanges === false`, הבורר בכותרת מוסתר.
  - `KanSocial.pickRange(r)` — קובע טווח, שומר אותו תחת `rangeKey` וטוען מחדש.
  - שאר העמודים לא משתנים: הם לא מעבירים `opts`.

- [ ] **Step 1: מימוש**

החלף את `getRange` ו־`setRange`:

```javascript
  function getRange(key) { try { return parseInt(localStorage.getItem(key || "pm_range") || "7", 10); } catch (e) { return 7; } }
  function setRange(r, key) { try { localStorage.setItem(key || "pm_range", r); } catch (e) {} }
```

ב־`buildHeader`, החלף את הבלוק שמתחיל ב־`(self.noData ? "" :`:

```javascript
          (self.noData ? "" :
            (self.headerRanges === false ? "" : '<div class="ranges">' + ranges + "</div>") +
            '<button class="iconbtn" id="btnRefresh" title="טעינה מחדש מהגיליון — לא מפעיל איסוף חדש">' + refreshIcon + "</button>") +
```

ובהאזנה ללחיצה על `.range`, החלף את `setRange(self.range);` ב־`setRange(self.range, self.rangeKey);`.

החלף את `init`:

```javascript
    init: function (page, onRender, opts) {
      opts = opts || {};
      this.page = page;
      this.theme = getTheme();
      this.rangeKey = opts.rangeKey || "pm_range";
      this.headerRanges = opts.headerRanges !== false;
      this.range = getRange(this.rangeKey);
      this.onRender = onRender;
      document.body.setAttribute("data-page", page);
      setTheme(this.theme);
      this.buildHeader();
      this.load();
    },

    // For a page whose range picker sits next to the section it changes
    // (competitors): the same fetch, remembered under the page's own key so it
    // never changes the range of every other page.
    pickRange: function (r) {
      this.range = r;
      setRange(r, this.rangeKey);
      this.load();
    },
```

- [ ] **Step 2: בדיקה ידנית ששאר העמודים לא השתנו**

Run: `venv/Scripts/python.exe -m uvicorn server:app --port 8430`. השרת קורא מהגיליון בקריאה בלבד.
פתח `http://localhost:8430/instagram`: הבורר מופיע בכותרת, ולחיצה על 30 טוענת מחדש. פתח `/youtube`: גם שם מסומן 30. הטווח המשותף נשמר.

- [ ] **Step 3: Commit**

```bash
git add social_dashboard/static/app.js
git commit -m "app.js: a page can own its range picker; refresh says it re-reads, not re-collects"
```

---

### Task 7: העמוד

**Files:**
- Modify: `social_dashboard/templates/competitors.html` (כתיבה מחדש של בלוק ה־`<script>`)
- Modify: `social_dashboard/static/app.css` (הוספה בסוף הקובץ)

**Interfaces:**
- Consumes: ה־payload של `competitors_page.build` (Task 5); `KanSocial.init(..., opts)` ו־`KanSocial.pickRange` (Task 6); `KS.*`.

- [ ] **Step 1: CSS**

הוסף בסוף `app.css`:

```css
/* ---------- competitors page ---------- */
.comp-grid{ display:grid; grid-template-columns:40px minmax(150px,1.3fr) 95px minmax(110px,1fr) 130px 80px 85px; gap:10px; align-items:center; }
.trow.comp-grid{ cursor:pointer; }
.comp-kan{ background:var(--pf-bg); }
.comp-sort{ font:inherit; color:inherit; background:none; border:none; cursor:pointer; text-align:center; padding:0; }
.comp-sort.active{ color:var(--text); }
.comp-muted{ color:var(--muted); }
.comp-warn{ font-size:12.5px; color:var(--bad); padding:6px 4px; }
.comp-empty{ font-size:13px; color:var(--muted); padding:8px 4px; }
.comp-more{ margin-top:10px; }
.comp-more > summary{ cursor:pointer; font-size:13px; color:var(--muted); padding:6px 4px; }
.comp-arena{ display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:8px; }
.comp-cards{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; padding:12px 16px; border-bottom:1px solid var(--line); }
.comp-card .v{ font-size:20px; font-weight:800; }
.comp-card .l{ font-size:12px; color:var(--muted); }
.comp-about{ font-size:12.5px; color:var(--muted); line-height:1.7; padding-inline-start:18px; margin:6px 0 0; }
@media (max-width:640px){
  .comp-grid{ grid-template-columns:28px minmax(0,1fr) 80px 100px; }
  .comp-hide-s{ display:none; }
  .comp-arena{ grid-template-columns:1fr; }
  .comp-cards{ grid-template-columns:1fr; }
}
```

- [ ] **Step 2: החלף את כל תוכן ה־`<script>` השני ב־`competitors.html`**

```html
<script>
(function () {
  var TYPE_HE = { IMAGE: "תמונה", VIDEO: "וידאו", CAROUSEL_ALBUM: "קרוסלה", Reel: "ריל", Photo: "תמונה", Carousel: "קרוסלה" };
  var sortKey = "followers";
  var last = null;

  function signedHtml(n) { return '<bdi dir="ltr">' + KS.signed(n) + '</bdi>'; }

  function postRow(p, withAccount) {
    var head = '<div class="hot-row-head"><span>' +
      (withAccount ? (p.is_kan ? "🔴 " : "") + KS.esc(p.name || p.username) + " · " : "") +
      (TYPE_HE[p.type] || KS.esc(p.type || "")) + '</span>' +
      '<span class="hot-time">' + KS.fmtDate(p.date) + (p.time ? " " + KS.esc(p.time) : "") + '</span></div>';
    var body = '<div class="hot-trig">' + KS.esc(p.caption || "(ללא כיתוב)") +
      '<br>❤️ ' + KS.fmt(p.likes) + ' · 💬 ' + KS.fmt(p.comments) + '</div>';
    return p.url
      ? '<a class="hot-row" href="' + KS.esc(p.url) + '" target="_blank" rel="noopener">' + head + body + '</a>'
      : '<div class="hot-row">' + head + body + '</div>';
  }

  // ---------- ראש העמוד ----------
  function sourcesLine(data) {
    var f = data.freshness, g = data.gaps || {};
    var parts = [];
    if (f) {
      parts.push("עודכן " + KS.fmtDate(f.date) + (f.pulled_at ? " " + KS.esc(f.pulled_at.slice(11, 16)) : ""));
      parts.push(f.updated + "/" + f.known + " חשבונות");
    }
    parts.push("בדיקת הסיקור: IG, FB, YouTube, X, TikTok — נכון לאיסוף הבוקר");
    var warn = "";
    if (f && f.missing.length) warn += '<div class="comp-warn">לא עודכנו באיסוף האחרון: ' + f.missing.map(KS.esc).join(", ") + '</div>';
    if (g.stale && g.stale.length) warn += '<div class="comp-warn">לא נטענו עכשיו, מוצגים מהטעינה הקודמת: ' + g.stale.map(KS.esc).join(", ") + '</div>';
    return '<p>' + parts.join(" · ") + '</p>' + warn;
  }

  // ---------- פערים ----------
  function why(g, names) {
    var l = g.lead, nm = names[l.username] || l.username;
    if (l.ratio) return "פי " + l.ratio.toFixed(1) + " מפוסט רגיל של " + nm;
    return KS.fmt(l.eng) + " לייקים+תגובות · הסף " + KS.fmt(l.threshold);
  }

  function gapRow(g, names) {
    var chips = g.posts.map(function (o) {
      var label = KS.esc(names[o.username] || o.username) + " · " + KS.fmt(o.eng);
      return o.url
        ? '<a class="km-theme" style="text-decoration:none;" href="' + KS.esc(o.url) + '" target="_blank" rel="noopener">' + label + '</a>'
        : '<span class="km-theme">' + label + '</span>';
    }).join("");
    return '<div class="hot-row"><div class="hot-row-head"><span>' +
      (g.n_outlets > 1 ? "רץ אצל " + g.n_outlets + " מתחרים" : KS.esc(names[g.lead.username] || g.lead.username)) +
      '</span><span class="hot-time">' + KS.fmtDate(g.date) + " " + KS.esc(g.time) + '</span></div>' +
      '<div class="hot-trig">' + KS.esc(g.caption) + '<br><span class="comp-muted">' + KS.esc(why(g, names)) + '</span></div>' +
      '<div class="km-themes" style="margin-top:6px;">' + chips + '</div></div>';
  }

  function gapsSection(data, names) {
    var g = data.gaps || { missed: [], exclusive: [] };
    var body;
    if (g.status === "unavailable") {
      body = '<div class="comp-warn">בדיקת הסיקור לא זמינה — לא נטען: ' + g.failed.map(KS.esc).join(", ") + '</div>';
    } else if (!g.missed.length) {
      body = '<div class="comp-empty">לא נמצאו סיפורים של 2 מתחרים ומעלה בלי התאמה אצלנו.</div>';
    } else {
      body = g.missed.map(function (x) { return gapRow(x, names); }).join("");
    }
    var excl = (g.exclusive || []).length
      ? '<details class="comp-more"><summary>💡 להיטים בלעדיים של מתחרים (' + g.exclusive.length + ') — תוכן של מתחרה אחד, לא פספוס</summary>' +
        g.exclusive.map(function (x) { return gapRow(x, names); }).join("") + '</details>'
      : "";
    return '<div class="section-label"><span>🕳 סיפורים שאין לנו · 72 השעות האחרונות</span><span></span></div>' +
      '<div class="panel mb-m"><div class="panel-sub" style="margin-bottom:10px;">סיפור שרץ חזק אצל 2 מתחרים ומעלה, ולא נמצא אצלנו פוסט עם מילים תואמות. זה חיפוש בטקסט הכיתובים, לא קביעה שהסיפור לא סוקר.</div>' +
      body + excl + '</div>';
  }

  // ---------- זירה ----------
  function arenaSection(data) {
    var a = data.arena || { posts: [] };
    if (!a.posts.length) return "";
    return '<div class="section-label"><span>🥊 הזירה · ' + KS.fmtDate(a.dates[0]) + '–' + KS.fmtDate(a.dates[1]) + '</span><span></span></div>' +
      '<div class="panel mb-m"><div class="panel-sub" style="margin-bottom:10px;">הפוסטים החזקים של כל החשבונות, כולל כאן, משלשום ומאתמול. מיון לפי לייקים+תגובות. לא תלוי בבורר הטווח.</div>' +
      '<div class="comp-arena">' + a.posts.map(function (p) { return postRow(p, true); }).join("") + '</div></div>';
  }

  // ---------- השוואה ----------
  function growthCell(g) {
    if (!g || g.status === "none") return '<span class="comp-muted">—</span>';
    if (g.status === "new") return '<span class="comp-muted" title="מדידה ראשונה: ' + KS.esc(g.base_date) + '">חדש במעקב</span>';
    var color = g.change > 0 ? "var(--ok)" : g.change < 0 ? "var(--bad)" : "var(--muted)";
    return '<span style="color:' + color + ';">' + signedHtml(g.change) +
      (g.pct == null ? "" : '<span class="comp-muted" style="font-size:11px;"> · <bdi dir="ltr">' + (g.pct >= 0 ? "+" : "") + g.pct.toFixed(2) + '%</bdi></span>') +
      '</span>';
  }

  function sortVal(c, k) {
    var g = c.growth || {};
    if (k === "change") return g.status === "ok" ? g.change : -Infinity;
    if (k === "pct") return g.status === "ok" && g.pct != null ? g.pct : -Infinity;
    if (k === "eng") return c.eng_per_1k;
    if (k === "ppd") return c.posts_per_day == null ? -Infinity : c.posts_per_day;
    return c.followers;
  }

  function sortBtn(k, label, cls) {
    return '<button class="comp-sort ' + (cls || "") + (sortKey === k ? " active" : "") + '" data-sort="' + k + '">' +
      label + (sortKey === k ? " ▾" : "") + '</button>';
  }

  function tableHtml(data) {
    var comps = data.competitors.slice().sort(function (a, b) {
      var x = sortVal(b, sortKey), y = sortVal(a, sortKey);
      return x === y ? b.followers - a.followers : (x > y ? 1 : -1);
    });
    var thead = '<div class="thead comp-grid"><span class="center">#</span><span>חשבון</span>' +
      sortBtn("followers", "עוקבים") + '<span class="center comp-hide-s">מגמה</span>' +
      sortBtn("change", "שינוי בטווח") + sortBtn("ppd", "פוסטים/יום", "comp-hide-s") +
      sortBtn("eng", "מעורבות/1K", "comp-hide-s") + '</div>';
    var rows = comps.map(function (c, i) {
      var spark = (c.spark && c.spark.length > 1)
        ? '<span style="display:block;max-width:120px;margin:0 auto;">' + KS.sparkSvg(c.spark, c.is_kan ? "#F7381B" : "#8b8fa3") + '</span>'
        : '<span class="comp-muted" style="font-size:11px;">—</span>';
      return '<div class="trow comp-grid' + (c.is_kan ? " comp-kan" : "") + '" data-u="' + KS.esc(c.username) + '" tabindex="0" role="button">' +
        '<span class="cell" style="color:var(--faint);">' + (i + 1) + '</span>' +
        '<span style="min-width:0;"><span class="t-title">' + (c.is_kan ? "🔴 " : "") + KS.esc(c.name) + '</span>' +
        '<span class="t-date">@' + KS.esc(c.username) + (c.change_1d ? ' · היום: ' + signedHtml(c.change_1d) : "") + '</span></span>' +
        '<span class="cell strong">' + KS.fmt(c.followers) + '</span>' +
        '<span class="center comp-hide-s">' + spark + '</span>' +
        '<span class="cell">' + growthCell(c.growth) + '</span>' +
        '<span class="cell comp-hide-s">' + (c.posts_per_day == null ? "—" : c.posts_per_day.toFixed(1)) + '</span>' +
        '<span class="cell comp-hide-s">' + c.eng_per_1k.toFixed(1) + '</span></div>';
    }).join("");
    return thead + rows;
  }

  function windowNote(data) {
    var w = data.window;
    if (!w) return "";
    var s = "שינוי עוקבים " + KS.fmtDate(w.base) + " → " + KS.fmtDate(w.end) + ", זהה לכל החשבונות";
    if (w.partial) s += ' · <b>טווח חלקי: ' + w.covered_days + ' ימים</b> (המעקב התחיל ' + KS.fmtDate(w.history_start) + ')';
    return s;
  }

  function rangePicker() {
    return '<div class="ranges">' + [7, 14, 30, 90].map(function (r) {
      return '<button class="range' + (r === KanSocial.range ? " active" : "") + '" data-r="' + r + '">' + r + '</button>';
    }).join("") + '</div>';
  }

  function card(label, value, sub) {
    return '<div class="comp-card"><div class="l">' + label + '</div><div class="v">' + value + '</div>' +
      (sub ? '<div class="l">' + sub + '</div>' : "") + '</div>';
  }

  function aboutSection() {
    return '<details class="comp-more"><summary>על הנתונים</summary><ul class="comp-about">' +
      '<li>המקור: Business Discovery של אינסטגרם — נתונים ציבוריים בלבד. אין צפיות, חשיפה או שמירות.</li>' +
      '<li>כאן חדשות: מהנתונים המלאים שלנו, באותם תאריכים.</li>' +
      '<li>שינוי עוקבים: בין תאריך הבסיס לתאריך הסוף שמעל הטבלה. התאריכים זהים לכל החשבונות.</li>' +
      '<li>מעורבות/1K: ממוצע לייקים+תגובות של 10 הפוסטים האחרונים ברגע האיסוף, חלקי אלף עוקבים. פוסטים טריים נספרים לפני שהבשילו, לכן זו הערכה.</li>' +
      '<li>פוסטים/יום: ממוצע על הימים המלאים בפיד השמור (עד 14). האיסוף מושך 25 פוסטים לחשבון ביום, ולכן אצל מי שמפרסם הרבה המספר עלול להיות נמוך מהאמת.</li>' +
      '<li>סיפורים שאין לנו: פוסט מתחרה מ־72 השעות האחרונות, עם פי 3 לפחות מהחציון של החשבון (ולא פחות מ־300), שלא נמצא לו אצלנו פוסט עם 4 מילים משותפות ב־96 השעות האחרונות. אתר ושידור אינם נבדקים.</li>' +
      '</ul></details>';
  }

  // ---------- מודל ----------
  function openFor(username) {
    var c = (last.competitors || []).filter(function (x) { return x.username === username; })[0];
    if (!c) return;
    var posts = c.posts || [];
    var body = posts.length
      ? KS.kmSection("הפוסטים החזקים · לפי לייקים+תגובות") +
        '<div class="panel-sub" style="padding:0 4px 8px;">מהפיד השמור (14 הימים האחרונים), גם כשנבחר טווח ארוך יותר.</div>' +
        posts.map(function (p) { return postRow(p, false); }).join("")
      : '<div class="comp-empty" style="padding:16px;">אין פוסטים בפיד השמור של החשבון.</div>';
    KS.openModal({
      iconHtml: KS.fillIcon("ig", "#fff", 19),
      iconBg: c.is_kan ? "var(--pf)" : "linear-gradient(135deg,#feda75,#d62976 45%,#962fbf 80%)",
      type: "@" + c.username + " · " + KS.fmt(c.followers) + " עוקבים",
      title: (c.is_kan ? "🔴 " : "") + (c.name || c.username),
      date: c.as_of ? "עודכן " + KS.fmtDate(c.as_of) : "",
      bodyHtml: body
    });
  }

  // ---------- רינדור ----------
  function render(data) {
    last = data;
    var names = {};
    (data.competitors || []).forEach(function (c) { names[c.username] = c.name || c.username; });
    var s = data.summary || {};
    document.getElementById("main").innerHTML =
      '<div class="page-head"><div><h1>מתחרים · Instagram</h1>' + sourcesLine(data) + '</div></div>' +
      gapsSection(data, names) +
      arenaSection(data) +
      '<div class="section-label"><span>📊 השוואה לאורך זמן</span><span></span></div>' +
      '<div class="table"><div class="table-head"><div><div class="panel-title">עוקבים וצמיחה</div>' +
      '<div class="panel-sub">' + windowNote(data) + '</div></div>' + rangePicker() + '</div>' +
      '<div class="comp-cards">' +
        card("צמיחת כאן בטווח", growthCell(s.kan_growth)) +
        card("דירוג כאן לפי עוקבים", "#" + s.kan_rank + ' <span class="comp-muted" style="font-size:13px;">מתוך ' + s.ranked + '</span>', "במדידה האחרונה, לא דירוג היסטורי") +
      '</div>' +
      '<div class="table-scroll" id="comp-body">' + tableHtml(data) + '</div></div>' +
      aboutSection();
  }

  var main = document.getElementById("main");
  main.addEventListener("click", function (e) {
    var r = e.target.closest("[data-r]");
    if (r) { KanSocial.pickRange(parseInt(r.getAttribute("data-r"), 10)); return; }
    var s = e.target.closest("[data-sort]");
    if (s) { sortKey = s.getAttribute("data-sort"); document.getElementById("comp-body").innerHTML = tableHtml(last); return; }
    if (e.target.closest("a") || e.target.closest("summary")) return;
    var row = e.target.closest(".trow[data-u]");
    if (row) openFor(row.getAttribute("data-u"));
  });
  main.addEventListener("keydown", function (e) {
    if (e.key !== "Enter") return;
    var row = e.target.closest(".trow[data-u]");
    if (row) openFor(row.getAttribute("data-u"));
  });

  KanSocial.init("competitors", render, { rangeKey: "pm_comp_range", headerRanges: false });
})();
</script>
```

- [ ] **Step 3: בדיקה בדפדפן מול הגיליון האמיתי (קריאה בלבד)**

Run: `venv/Scripts/python.exe -m uvicorn server:app --port 8430`. פתח `http://localhost:8430/competitors` ובדוק:
- אין בורר טווח בכותרת. בורר 7/14/30/90 מופיע ליד "עוקבים וצמיחה".
- מעבר בין 7 ל־90 משנה את "צמיחת כאן בטווח" ואת עמודת השינוי בלבד. הזירה והפערים נשארים זהים.
- ב־90 מופיע "טווח חלקי".
- לחיצה על כותרת עמודה ממיינת. לחיצה על שורה, וגם Enter על שורה ממוקדת, פותחת את המודל. Escape סוגר.
- `/instagram` עדיין מציג בורר בכותרת, והטווח שלו לא השתנה.
- רוחב 390, 768 ו־1440 פיקסלים, בהיר וכהה: אין גלילה אופקית של הדף, ושום סימן +/− לא מתהפך.
- ה־Console נקי משגיאות.

- [ ] **Step 4: Commit**

```bash
git add social_dashboard/templates/competitors.html social_dashboard/static/app.css
git commit -m "competitors page: missed vs exclusive, fixed arena, local range, sortable table, about"
```

---

### Task 8: אימות מול הגיליון, ROADMAP

**Files:**
- Modify: `docs/ROADMAP.md` (פריט חדש תחת "Open — dashboard")

- [ ] **Step 1: השוואה חיה בקריאה בלבד**

צור בתיקיית ה־scratchpad את `verify_stage1.py`:

```python
import sys
sys.path.insert(0, r"E:\social-dashboard-kan\social_dashboard")
sys.stdout.reconfigure(encoding="utf-8")
import gsheets, competitors_page as C, aggregate as A
d = gsheets.get_data(keys=("competitors", "competitor_posts", "facebook", "instagram",
                           "youtube", "twitter", "tiktok", "followers"))
print("sources:", d.source_status())
rows = {str(r["date"])[:10]: A._int(r["ig_followers"]) for r in d["followers"] if A._int(r.get("ig_followers"))}
arenas = []
for days in (7, 14, 30, 90):
    b = C.build(d, days)
    k = b["summary"]["kan_growth"]
    manual = rows[k["end_date"]] - rows[k["base_date"]]
    print(days, "kan", k["change"], "manual", manual, "base", k["base_date"], "partial", b["window"]["partial"])
    assert k["change"] == manual
    arenas.append(b["arena"])
assert all(a == arenas[0] for a in arenas), "arena moved with the range"
print("gaps:", len(b["gaps"]["missed"]), "missed,", len(b["gaps"]["exclusive"]), "exclusive; status", b["gaps"]["status"])
print("freshness:", b["freshness"])
```

Run: `venv/Scripts/python.exe <scratchpad>/verify_stage1.py`
Expected:
- הצמיחה של כאן שונה בכל טווח ושווה להפרש הידני. ב־27.9 הערכים היו 482 / 1,034 / 1,581, וב־90 ימים המספר קטן מ־4,947 כי הבסיס הוא 13.7.
- ב־90 מוצג `partial True`.
- אין `AssertionError`.

- [ ] **Step 2: ROADMAP**

תחת `## Open — dashboard`, הוסף אחרי פריט 3:

```markdown
### 19. Competitors — stages 2–4 of the redesign

Stage 1 (accuracy) shipped: one date pair for every account and Kan, fixed
arena window, gaps split into missed (2+ rivals) and exclusive, and a failed
sheet read shown as unavailable instead of "no gaps". Still open, in order —
spec `docs/superpowers/specs/2026-09-27-competitors-design.md`:
- **2** — intraday run at the sniffer hours (11/14/17/20/23), writing only the
  posts tab and two new tabs (post history, gap candidates). The daily run is
  untouched.
- **3** — "now at rivals" (pace vs the account's own posts at the same age) and
  engagement/1K from posts at ~24h. Needs a week of history.
- **4** — Telegram for missed stories, only after 2–3 weeks of logged
  candidates and a precision check (≥30 marked by Ben).
```

- [ ] **Step 3: Commit**

```bash
git add docs/ROADMAP.md
git commit -m "roadmap: competitors stages 2-4"
```

---

### Task 9 (שלב 1ב): ארבעה חשבונות חדשים

**Files:**
- Modify: `competitors_collector.py:49-53`

- [ ] **Step 1: עדכון הרשימה**

```python
COMPETITORS = [
    'n12news', 'ynetgram', '13newsil', 'yedioth', 'israelhayom',
    'haaretz', 'wallanews', 'maarivonline', 'globesnews',
    'm_laradar', 'yomi_news',
    # נוספו 2026-09-27, אומתו ב־probe_discovery (ריצות 36320788572, 36321068432)
    'i24news_he', 'now14israel', 'hamal_news', 'kikarhashabat',
]
```

עדכן גם את שורת התאריך בדוקסטרינג: "הרשימה אומתה חיה מול ה-API ב-2026-07-13; ארבעה חשבונות נוספו ב-2026-09-27."

- [ ] **Step 2: Commit**

```bash
git add competitors_collector.py
git commit -m "competitors: track i24news_he, now14israel, hamal_news, kikarhashabat"
```

- [ ] **Step 3: מיזוג לפני הריצה הבאה — באישור בן בלבד**

פתיחת PR ומיזוג ל־main הם החלטה של בן. אחרי המיזוג, **לפני** 08:30 של הבוקר הבא:

Run (מתוך שורש הריפו): `python verify_collector.py snap competitors`

- [ ] **Step 4: אחרי הריצה היומית**

Run: `python verify_collector.py check competitors`
Expected: exit 0. אין עמודות שזזו. מספר השורות היומי עלה מ־11 ל־15.

אחר כך בעמוד: ארבעת החשבונות מופיעים עם "חדש במעקב" בעמודת השינוי, ונכנסים לדירוג הצמיחה של 7 ימים כשיש להם מדידה בתחילת החלון (יום אחד של מרווח), כלומר מהיום השביעי. בטווחים ארוכים יותר, בהתאם. ה־`eng_per_1k` וה־`posts` שלהם מופיעים מהיום הראשון.
