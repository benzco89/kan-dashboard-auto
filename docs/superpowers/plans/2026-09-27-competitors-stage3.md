# עמוד המתחרים — שלב 3: "עכשיו אצל המתחרים", מעורבות בגיל 24 שעות, מגמות — תוכנית ביצוע

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** שלושה שימושים ב"היסטוריית פוסטים מתחרים":
1. החלק "עכשיו אצל המתחרים" — פוסט צעיר שרץ מהר מהרגיל של החשבון שלו באותו גיל.
2. מעורבות ל־1K לפי פוסטים בגיל 24 שעות.
3. מגמת ספירות לכל פוסט בחלון החשבון.

החלק "עכשיו" **נבנה עכשיו ונפתח רק אחרי כיול**, במתג מפורש.

**Architecture:**
- **החישובים** — פונקציות טהורות ב־`competitors_page.py` על ההיסטוריה.
- **ההיסטוריה של כאן** — הריצה התוך־יומית רושמת להיסטוריה גם את ספירות האינסטגרם של כאן, כ־`username = kan_news`. בלי זה אין לכאן מספר בגיל 24 שעות: הגיליון של כאן נמשך פעם ביום ושומר רק את הספירה האחרונה.
- **סקריפט כיול** — קריאה בלבד, רץ בסביבות 4.10. הוא מודד דיוק מול איפה שהפוסט נחת בבגרותו, ולא לפי כמה פעמים משהו נדלק.

**Tech Stack:** Python 3.10, vanilla JS, CSS.

**Spec:** `docs/superpowers/specs/2026-09-27-competitors-design.md` §3.2, §3.5, §3.6, §6 (שלב 3). החלטת בן, 27.9: לבנות עכשיו ולכייל בעוד שבוע.

## Global Constraints

- **החלק "עכשיו" לא מציג אף ציון** כל עוד `NOW_CALIBRATED = False`, גם אחרי שבוע של היסטוריה. במצב הזה הוא מציג "נאסף" או "ממתין לכיול".
- **ספים (מהמפרט §3.2, ממתינים לכיול):** גיל פחות מ־24 שעות, בסיס בטווח של ±2 שעות, לפחות 8 נקודות בסיס, פי 2 לפחות, עד 10 פוסטים. שבוע של היסטוריה לפני פתיחה.
- **מעורבות בגיל 24 שעות:** תצפית אחת לכל פוסט, בגיל 20–30 שעות, הקרובה ביותר ל־24. צריך לפחות 5 פוסטים; אחרת המספר חוזר לממוצע של צילום המצב ומסומן `estimate`.
- כאן (`kan_news`) לא מופיע לעולם ב"עכשיו אצל המתחרים".
- הריצה התוך־יומית ממשיכה לכתוב רק ל־`WRITTEN_TABS`. השורות של כאן נכנסות להיסטוריה בלבד.
- כל מספר עם סימן בממשק עטוף ב־`<bdi dir="ltr">`.
- בדיקות הדשבורד רצות עם `social_dashboard/venv/Scripts/python.exe`, מתוך `social_dashboard/`. בדיקות שורש הריפו רצות עם `python`.
- Branch: `competitors-stage3`.

## מפת קבצים

| קובץ | שינוי |
|---|---|
| `social_dashboard/competitors_page.py` | היסטוריה: `history_by_post`, `posts_by_account`, `baseline_at`, `now_at_rivals`, `eng_at_24h`, מגמות. `build` משתמש בכולם |
| `social_dashboard/test_competitors.py` | בדיקות |
| `competitors_intraday.py`, `test_competitors_intraday.py` | `kan_history_rows`, ומשיכת ספירות של כאן |
| `social_dashboard/gsheets.py`, `social_dashboard/server.py` | לשונית `competitor_history` |
| `social_dashboard/templates/competitors.html`, `social_dashboard/static/app.css` | חלק "עכשיו", תאי מעורבות, מגמות במודל |
| `social_dashboard/analyze_now_thresholds.py` — **חדש** | סקריפט הכיול |
| `docs/ROADMAP.md`, המפרט | סטטוס |

---

### Task 1: חישובי ההיסטוריה ב־competitors_page

**Files:**
- Modify: `social_dashboard/competitors_page.py`
- Test: `social_dashboard/test_competitors.py`

**Interfaces:**
- Produces:
  - `history_by_post(rows) -> {post_id: [(pulled_at, age_h, eng, username), ...]}` — ממוין לפי זמן המשיכה.
  - `posts_by_account(hist) -> {username: [(post_id, obs), ...]}`.
  - `baseline_at(by_user, user, pid, age) -> list[int]`.
  - `now_at_rivals(hist, comp_posts, names, now) -> {"status": "building"|"calibrating"|"ok", "since", "ready_on", "items", ["run_at"]}`.
    - שדות כל item: `username`, `name`, `post_id`, `caption`, `url`, `age_h`, `eng`, `baseline`, `ratio`, `n_base`.
  - `eng_at_24h(posts) -> (median, n)`.
  - שורת חשבון ב־`build` מקבלת `eng_basis` (`"24h"` או `"estimate"`).
  - כל פוסט (`post_view`) מקבל `post_id`. פוסטים בחלון החשבון מקבלים גם `trend` (רשימת ספירות, או `[]`).
  - ה־payload מקבל `now`.

- [ ] **Step 1: כתוב את הבדיקות שנכשלות**

ב־`test_competitors.py`, מעל שלוש שורות הסיכום:

```python
print("\nhistory: now at rivals, engagement at 24h, trends\n" + "-" * 62)


def hrow(pid, user, pulled, age, likes):
    return {"post_id": pid, "username": user, "pulled_at": pulled, "age_h": str(age),
            "likes": str(likes), "comments": "0"}


# ten older posts of aaa, each seen at 5h (100) and at 24h (400)
HIST = []
for i in range(10):
    t0 = datetime(2026, 9, 19, 10) + timedelta(days=i % 7, hours=i)
    HIST.append(hrow(f"a{i}", "aaa", (t0 + timedelta(hours=5)).strftime("%Y-%m-%d %H:%M"), 5, 100))
    HIST.append(hrow(f"a{i}", "aaa", (t0 + timedelta(hours=24)).strftime("%Y-%m-%d %H:%M"), 24, 400))
LAST = "2026-09-27 11:05"
HIST += [hrow("hot", "aaa", LAST, 5.5, 350),                   # 3.5x its account at that age
         hrow("calm", "aaa", LAST, 4.5, 120),                  # 1.2x
         hrow("old", "aaa", LAST, 30, 5000),                   # past 24h
         hrow("stale", "aaa", "2026-09-27 08:05", 5, 900),     # not in the latest run
         hrow("k1", "kan_news", LAST, 5, 5000)]                # Kan is never "at rivals"
H = C.history_by_post(HIST)

check("observations are grouped per post, in pull order", [o[1] for o in H["a0"]], [5.0, 24.0])
young = C.history_by_post([r for r in HIST if r["pulled_at"] == LAST])
b = C.now_at_rivals(young, [], {}, NOW)
check("a week of history comes first", (b["status"], b["ready_on"]), ("building", "2026-10-04"))
check("with a week of history but before calibration, nothing is scored",
      C.now_at_rivals(H, [], {}, NOW)["status"], "calibrating")
C.NOW_CALIBRATED = True
try:
    n = C.now_at_rivals(H, [{"post_id": "hot", "caption": "כותרת", "permalink": "u"}], {"aaa": "AAA"}, NOW)
finally:
    C.NOW_CALIBRATED = False
check("only a young post running well ahead of its own account at that age",
      [i["post_id"] for i in n["items"]], ["hot"])
check("measured against the account's other posts at the same age",
      (n["items"][0]["ratio"], n["items"][0]["baseline"], n["items"][0]["n_base"]), (3.5, 100, 12))
check("with its caption, link and account name",
      (n["items"][0]["caption"], n["items"][0]["url"], n["items"][0]["name"]), ("כותרת", "u", "AAA"))

check("engagement at ~24h: one observation per post, within 20-30h",
      C.eng_at_24h(C.posts_by_account(H)["aaa"]), (400, 11))

DATA3 = dict(DATA, competitor_history=HIST, competitor_posts=DATA["competitor_posts"] + [
    {"post_id": "a0", "username": "aaa", "date": "2026-09-26", "time": "10:00", "likes": "99999",
     "comments": "0", "caption": "x", "permalink": "p", "pulled_at": "2026-09-26 23:00"}])
b3 = C.build(DATA3, 7, today=TODAY, now=NOW)
aaa = [c for c in b3["competitors"] if c["username"] == "aaa"][0]
check("engagement/1K comes from posts at ~24h once there are enough",
      (aaa["eng_basis"], aaa["eng_per_1k"]), ("24h", round(400 / aaa["followers"] * 1000, 2)))
check("without enough history it stays an estimate",
      [c["eng_basis"] for c in b3["competitors"] if c["username"] == "bbb"], ["estimate"])
check("a post with history carries its count trend",
      [p["trend"] for p in aaa["posts"] if p["post_id"] == "a0"], [[100, 400]])
check("the page reports the now-section state", b3["now"]["status"], "calibrating")
```

- [ ] **Step 2: הרץ ווודא שהבדיקות נכשלות**

Run: `cd social_dashboard && venv/Scripts/python.exe test_competitors.py`
Expected: `AttributeError: module 'competitors_page' has no attribute 'history_by_post'`

- [ ] **Step 3: מימוש**

ב־`competitors_page.py`:

שנה את `from datetime import datetime, timedelta` ל־`from datetime import date, datetime, timedelta`.

מתחת לבלוק `CANDIDATE_COLUMNS` הוסף:

```python
# ---------- שלב 3: היסטוריית הספירות ----------
# הספים של "עכשיו אצל המתחרים" לקוחים מהמפרט (3.2) ועוד לא כוילו. NOW_CALIBRATED
# נשאר False עד שמריצים analyze_now_thresholds.py על שבוע של היסטוריה ומוודאים
# שמה שנדלק צעיר אכן נוחת גבוה בבגרותו - אחרת החלק היה נדלק מעצמו על ניחוש.
NOW_MAX_AGE_H = 24        # "עכשיו": פוסט בן פחות מיממה בריצה האחרונה
NOW_AGE_TOL_H = 2         # בסיס: שאר הפוסטים של החשבון, בגיל ±2 שעות
NOW_MIN_BASE = 8          # פחות נקודות בסיס = אין ציון
NOW_MIN_RATIO = 2.0
NOW_TOP = 10
NOW_HISTORY_DAYS = 7
NOW_CALIBRATED = False
ENG24_MIN_H, ENG24_MAX_H = 20, 30
ENG24_MIN_POSTS = 5
```

ב־`post_view`, הוסף למילון המוחזר את השורה הבאה, לפני `"url"`:

```python
            "post_id": str(p.get("post_id") or p.get("media_id") or ""),
```

הוסף את הבלוק הבא לפני `# ---------- העמוד ----------`:

```python
# ---------- היסטוריה ----------

def history_by_post(rows):
    """{post_id: [(pulled_at, age_h, eng, username), ...]}, לפי זמן המשיכה."""
    out = {}
    for r in rows:
        pid, pulled = str(r.get("post_id", "")).strip(), str(r.get("pulled_at", "")).strip()
        if pid and pulled:
            out.setdefault(pid, []).append(
                (pulled, A._num(r.get("age_h")), _eng(r), str(r.get("username", "")).strip()))
    for obs in out.values():
        obs.sort()
    return out


def posts_by_account(hist):
    by = {}
    for pid, obs in hist.items():
        by.setdefault(obs[-1][3], []).append((pid, obs))
    return by


def _closest(obs, age, tol):
    best = min(obs, key=lambda o: abs(o[1] - age), default=None)
    return best if best is not None and abs(best[1] - age) <= tol else None


def baseline_at(by_user, user, pid, age):
    """הספירות של שאר הפוסטים של החשבון כשהיו בני age שעות (±NOW_AGE_TOL_H),
    תצפית אחת לפוסט - הקרובה ביותר לגיל."""
    base = []
    for other, obs in by_user.get(user, []):
        if other != pid:
            c = _closest(obs, age, NOW_AGE_TOL_H)
            if c:
                base.append(c[2])
    return base


def now_at_rivals(hist, comp_posts, names, now):
    """פוסטים של מתחרים, בני פחות מיממה בריצה האחרונה, שרצים מהר מהרגיל של
    החשבון שלהם באותו גיל. ההשוואה היא לאותו חשבון ולאותו גיל: פוסט בן 3 שעות
    אינו בר השוואה לפוסט בן יום, ווואלה אינה N12."""
    if not hist:
        return {"status": "building", "since": None, "ready_on": None, "items": []}
    since = min(obs[0][0] for obs in hist.values())[:10]
    ready_on = str(date.fromisoformat(since) + timedelta(days=NOW_HISTORY_DAYS))
    base = {"since": since, "ready_on": ready_on, "items": []}
    if str(now.date()) < ready_on:
        return dict(base, status="building")
    if not NOW_CALIBRATED:
        return dict(base, status="calibrating")

    latest = max(obs[-1][0] for obs in hist.values())
    by_user = posts_by_account(hist)
    meta = {str(p.get("post_id", "")): p for p in comp_posts}
    items = []
    for pid, obs in hist.items():
        pulled, age, eng, user = obs[-1]
        if pulled != latest or age >= NOW_MAX_AGE_H or user == "kan_news":
            continue
        b = baseline_at(by_user, user, pid, age)
        med = A._median(b) if len(b) >= NOW_MIN_BASE else 0
        if not med or eng / med < NOW_MIN_RATIO:
            continue
        p = meta.get(pid, {})
        items.append({"username": user, "name": names.get(user, user), "post_id": pid,
                      "caption": A._clean_caption(p.get("caption", ""))[:200],
                      "url": p.get("permalink", ""), "age_h": age, "eng": eng,
                      "baseline": med, "ratio": round(eng / med, 1), "n_base": len(b)})
    items.sort(key=lambda x: -x["ratio"])
    return dict(base, status="ok", run_at=latest, items=items[:NOW_TOP])


def eng_at_24h(posts):
    """חציון הספירה בגיל ~24 שעות ומספר הפוסטים שנמדדו. תצפית אחת לפוסט,
    בגיל 20-30 שעות, הקרובה ביותר ל-24."""
    vals = []
    for _pid, obs in posts:
        near = [o for o in obs if ENG24_MIN_H <= o[1] <= ENG24_MAX_H]
        if near:
            vals.append(min(near, key=lambda o: abs(o[1] - 24))[2])
    return (A._median(vals) if vals else 0), len(vals)
```

ב־`build`:

1. מתחת לשורה `fw = feed_window(comp_posts, days, today)` הוסף:

```python
    hist = history_by_post(data.get("competitor_history", []) or [])
    hist_users = posts_by_account(hist)

    def eng_fields(user, followers_now, fallback):
        med, n = eng_at_24h(hist_users.get(user, []))
        if n >= ENG24_MIN_POSTS and followers_now:
            return round(med / followers_now * 1000, 2), "24h"
        return fallback, "estimate"

    def with_trend(v):
        obs = hist.get(v["post_id"], [])
        return dict(v, trend=[o[2] for o in obs] if len(obs) > 1 else [])
```

2. בלולאת המתחרים, לפני `rows.append({`, הוסף:

```python
        eng, basis = eng_fields(u, A._int(latest.get("followers")),
                                round(A._num(latest.get("eng_per_1k")), 2))
```

החלף את השורה `"eng_per_1k": round(A._num(latest.get("eng_per_1k")), 2),` ב־`"eng_per_1k": eng, "eng_basis": basis,`. את `"posts": sorted((post_view(p, "caption", u, names[u], False) for p in own), key=lambda x: -x["eng"])[:POSTS_PER_ACCOUNT],` החלף ב:

```python
            "posts": [with_trend(v) for v in sorted(
                (post_view(p, "caption", u, names[u], False) for p in own),
                key=lambda x: -x["eng"])[:POSTS_PER_ACCOUNT]],
```

3. בשורה של כאן, לפני `rows.append({` של `kan_news`, הוסף:

```python
    kan_eng, kan_basis = eng_fields("kan_news", kan_followers,
                                    round(avg / kan_followers * 1000, 2) if kan_followers else 0)
```

החלף את `"eng_per_1k": round(avg / kan_followers * 1000, 2) if kan_followers else 0,` ב־`"eng_per_1k": kan_eng, "eng_basis": kan_basis,`. את ה־`"posts"` של כאן עטוף כך: `"posts": [with_trend(v) for v in sorted(...)[:POSTS_PER_ACCOUNT]],`, כלומר אותו ביטוי `sorted` בדיוק, בתוך `with_trend`.

4. במילון המוחזר, אחרי `"arena": ...`, הוסף:

```python
        "now": now_at_rivals(hist, comp_posts, names, now),
```

- [ ] **Step 4: הרץ ווודא שהבדיקות עוברות**

Run: `cd social_dashboard && venv/Scripts/python.exe test_competitors.py`
Expected: `77/77 passed`. הרץ גם את כל `test_*.py` בתיקייה.

- [ ] **Step 5: Commit**

```bash
git add social_dashboard/competitors_page.py social_dashboard/test_competitors.py
git commit -m "competitors_page: now-at-rivals (off until calibrated), engagement at 24h, count trends"
```

---

### Task 2: היסטוריה לכאן בריצה התוך־יומית

**Files:**
- Modify: `competitors_intraday.py`
- Test: `test_competitors_intraday.py`

**Interfaces:**
- Produces: `kan_history_rows(ig_media, pulled_at) -> list[dict]` — אותן עמודות כמו `HISTORY_COLUMNS`, עם `username = "kan_news"`.

- [ ] **Step 1: כתוב את הבדיקות שנכשלות**

ב־`test_competitors_intraday.py`, מעל שורות הסיכום:

```python
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
```

- [ ] **Step 2: הרץ ווודא שהבדיקות נכשלות**

Run: `python test_competitors_intraday.py`
Expected: `AttributeError: module 'competitors_intraday' has no attribute 'kan_history_rows'`

- [ ] **Step 3: מימוש**

ב־`competitors_intraday.py`, החלף את `_il_date` בשתי הפונקציות האלה:

```python
def _il_dt(ts):
    """חותמת ה-API -> "YYYY-MM-DD HH:MM" בשעון ישראל; "" אם אינה קריאה."""
    try:
        return (datetime.fromisoformat(str(ts).replace("Z", "+00:00").replace("+0000", "+00:00"))
                .astimezone(CC.IL_TZ).strftime("%Y-%m-%d %H:%M"))
    except ValueError:
        return ""


def _il_date(ts):
    return _il_dt(ts)[:10]
```

והוסף אחרי `kan_fresh_rows`:

```python
def kan_history_rows(ig_media, pulled_at):
    """ספירות האינסטגרם של כאן לאותה היסטוריה (username kan_news), כדי שמעורבות/1K
    של כאן תימדד כמו של המתחרים - בגיל ~24 שעות. הגיליון של כאן נמשך פעם ביום
    ושומר רק את הספירה האחרונה, אז אי אפשר לקחת אותה משם."""
    pulled = datetime.strptime(pulled_at, "%Y-%m-%d %H:%M")
    out = []
    for m in ig_media:
        posted_at = _il_dt(m.get("timestamp"))
        if not posted_at or not m.get("id"):
            continue
        age = (pulled - datetime.strptime(posted_at, "%Y-%m-%d %H:%M")).total_seconds() / 3600
        out.append({"post_id": str(m["id"]), "username": "kan_news", "posted_at": posted_at,
                    "pulled_at": pulled_at, "age_h": round(age, 1),
                    "likes": int(m.get("like_count") or 0),
                    "comments": int(m.get("comments_count") or 0)})
    return out
```

ב־`fetch_kan_posts`, שנה את `"fields": "caption,timestamp"` של אינסטגרם ל־`"fields": "id,caption,timestamp,like_count,comments_count"`. עדכן את הדוקסטרינג: "טקסט, תאריך, ובאינסטגרם גם ספירות להיסטוריה".

ב־`main`, החלף את `hist = history_rows(post_rows, run_at)` ב:

```python
    hist = history_rows(post_rows, run_at) + kan_history_rows(ig_media, run_at)
```

- [ ] **Step 4: הרץ ווודא שהבדיקות עוברות**

Run: `python test_competitors_intraday.py`
Expected: `19/19 passed`

- [ ] **Step 5: Commit**

```bash
git add competitors_intraday.py test_competitors_intraday.py
git commit -m "competitors_intraday: Kan's IG counts into the history, so Kan is measured at 24h too"
```

---

### Task 3: העמוד

**Files:**
- Modify: `social_dashboard/gsheets.py` (`LAZY_SHEETS`), `social_dashboard/server.py` (`_PAGE_SHEETS["competitors"]`)
- Modify: `social_dashboard/templates/competitors.html`, `social_dashboard/static/app.css`

- [ ] **Step 1: חיבור הלשונית**

ב־`gsheets.py`, ב־`LAZY_SHEETS`, אחרי `"gap_candidates"` הוסף:

```python
    "competitor_history": "היסטוריית פוסטים מתחרים",
```

ב־`server.py`, הוסף `"competitor_history"` לסוף `_PAGE_SHEETS["competitors"]`.

- [ ] **Step 2: חלק "עכשיו" ופריסה בשתי עמודות**

ב־`competitors.html`, הוסף לפני `function gapsSection`:

```javascript
  // ---------- עכשיו אצל המתחרים ----------
  function nowSection(data) {
    var n = data.now || { status: "building", items: [] };
    var body;
    if (n.status === "building") {
      body = '<div class="comp-empty">נאסף' + (n.since ? " מאז " + KS.fmtDate(n.since) : "") +
        (n.ready_on ? ", יהיה זמין ב־" + KS.fmtDate(n.ready_on) : "") +
        ' — צריך שבוע של ספירות כדי לדעת מה "רגיל" לכל חשבון.</div>';
    } else if (n.status === "calibrating") {
      body = '<div class="comp-empty">יש מספיק היסטוריה. החלק ייפתח אחרי כיול הספים מול הנתונים.</div>';
    } else if (!n.items.length) {
      body = '<div class="comp-empty">אין כרגע פוסט שרץ פי 2 ומעלה מהרגיל של החשבון שלו.</div>';
    } else {
      body = n.items.map(function (it) {
        var head = '<div class="hot-row-head"><span>' + KS.esc(it.name) + '</span>' +
          '<span class="hot-time">בן ' + Math.round(it.age_h) + ' שעות</span></div>';
        var txt = '<div class="hot-trig">' + KS.esc(it.caption || "(ללא כיתוב)") +
          '<br><span class="comp-muted">פי ' + it.ratio.toFixed(1) + ' מפוסט רגיל של ' + KS.esc(it.name) +
          ' בגיל ' + Math.round(it.age_h) + ' שעות · ' + KS.fmt(it.eng) + ' לייקים+תגובות</span></div>';
        return it.url
          ? '<a class="hot-row" href="' + KS.esc(it.url) + '" target="_blank" rel="noopener">' + head + txt + '</a>'
          : '<div class="hot-row">' + head + txt + '</div>';
      }).join("");
    }
    return '<div class="section-label"><span>🔥 עכשיו אצל המתחרים · 24 השעות האחרונות</span><span></span></div>' +
      '<div class="panel mb-m"><div class="panel-sub" style="margin-bottom:10px;">פוסטים שרצים מהר מהרגיל של החשבון שלהם, בהשוואה לפוסטים שלו באותו גיל.</div>' +
      body + '</div>';
  }
```

ב־`render`, החלף את השורה `gapsSection(data, names) +` ב:

```javascript
      '<div class="comp-top"><div>' + nowSection(data) + '</div><div>' + gapsSection(data, names) + '</div></div>' +
```

- [ ] **Step 3: מעורבות ומגמות**

ב־`tableHtml`:
- החלף את `sortBtn("eng", "מעורבות/1K (הערכה)", "comp-hide-s")` ב־`sortBtn("eng", "מעורבות/1K", "comp-hide-s")`.
- החלף את `'<span class="cell comp-hide-s">' + c.eng_per_1k.toFixed(1) + '</span></div>'` ב:

```javascript
        '<span class="cell comp-hide-s">' + c.eng_per_1k.toFixed(1) +
          (c.eng_basis === "estimate" ? '<span class="comp-muted" title="הערכה: עוד אין מספיק היסטוריה, אז זה ממוצע 10 הפוסטים האחרונים ברגע האיסוף, כולל טריים">~</span>' : "") +
        '</span></div>';
```

ב־`postRow`, החלף את `'<br>❤️ ' + KS.fmt(p.likes) + ' · 💬 ' + KS.fmt(p.comments) + '</div>';` ב:

```javascript
      '<br>❤️ ' + KS.fmt(p.likes) + ' · 💬 ' + KS.fmt(p.comments) +
      (p.trend && p.trend.length > 1
        ? '<span class="comp-trend" title="לייקים+תגובות לאורך המשיכות">' + KS.sparkSvg(p.trend, "#8b8fa3") + '</span>'
        : "") + '</div>';
```

ב־`aboutSection`, החלף את פריט "מעורבות/1K" ב:

```javascript
      '<li>מעורבות/1K: חציון לייקים+תגובות של פוסטים בגיל ~24 שעות, חלקי אלף עוקבים — כל החשבונות, וכאן, נמדדים באותו גיל. ~ = עוד אין מספיק היסטוריה, והמספר הוא ממוצע 10 הפוסטים האחרונים ברגע האיסוף (הערכה).</li>' +
```

והוסף אחריו:

```javascript
      '<li>עכשיו אצל המתחרים: פוסט בן פחות מיממה, שהספירה שלו גבוהה לפחות פי 2 מהחציון של שאר הפוסטים של אותו חשבון כשהיו באותו גיל (±2 שעות, לפחות 8 פוסטים להשוואה).</li>' +
```

- [ ] **Step 4: CSS**

בסוף בלוק המתחרים ב־`app.css` (לפני ה־`@media (max-width:640px)` שלו), הוסף:

```css
.comp-top{ display:grid; grid-template-columns:7fr 5fr; gap:16px; align-items:start; }
.comp-top > div{ min-width:0; }
.comp-trend{ display:inline-block; width:90px; vertical-align:middle; margin-inline-start:8px; }
@media (max-width:1000px){ .comp-top{ grid-template-columns:1fr; } }
```

- [ ] **Step 5: בדיקה**

- `node --check` ל־`static/app.js` ולסקריפט המוטמע של `competitors.html` (חלץ אותו לקובץ זמני).
- כל `test_*.py` בתיקיית הדשבורד.
- הפעל את השרת (`venv/Scripts/python.exe -m uvicorn server:app --port 8433`, קריאה בלבד) ובדוק:
  - `curl -s "localhost:8433/api/competitors?days=7"` מחזיר `now.status == "building"`, עם `ready_on` שבוע אחרי 27.9.
  - לכל שורה יש `eng_basis`.
  - אחר כך עצור את השרת.

- [ ] **Step 6: Commit**

```bash
git add social_dashboard/gsheets.py social_dashboard/server.py social_dashboard/templates/competitors.html social_dashboard/static/app.css
git commit -m "competitors page: now-at-rivals section, engagement at 24h marked when estimated, count trends"
```

---

### Task 4: סקריפט הכיול ומסמכים

**Files:**
- Create: `social_dashboard/analyze_now_thresholds.py`
- Modify: `docs/ROADMAP.md` (פריט 19), `docs/superpowers/specs/2026-09-27-competitors-design.md` (§3.2)

- [ ] **Step 1: הסקריפט**

```python
"""כיול "עכשיו אצל המתחרים" - קריאה בלבד. מריצים אחרי שבוע של היסטוריה.

הקריטריון הוא דיוק מול אמת שאינה תלויה בטריגר, כמו בכיול הסניפר: פוסט שהיה
צעיר "פי X מהרגיל" - באיזה אחוזון של החשבון שלו הוא נחת בבגרותו? לא "כמה
פעמים זה נדלק". פוסט נחשב "נדלק" בסף t אם באיזושהי משיכה לפני גיל 24 שעות
היחס שלו עבר את t.

    cd social_dashboard && venv/Scripts/python.exe analyze_now_thresholds.py

אחרי הכיול: לעדכן את NOW_MIN_RATIO (ואם צריך NOW_AGE_TOL_H / NOW_MIN_BASE)
ב-competitors_page.py, להעביר את NOW_CALIBRATED ל-True ולתעד את המספרים
ב-ROADMAP.
"""

import sys

import competitors_page as C
import gsheets

MATURE_FINAL_H = 40        # "בגרות": התצפית האחרונה של הפוסט בגיל 40 שעות ומעלה
MIN_FINALS = 10            # חשבון עם פחות פוסטים בשלים לא נכנס לכיול
THRESHOLDS = (1.5, 2.0, 2.5, 3.0)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    rows = gsheets.get_data(keys=("competitor_history",)).get("competitor_history", [])
    hist = C.history_by_post(rows)
    by_user = C.posts_by_account(hist)
    peaks = []          # (max early ratio, final percentile, account)
    unscored = 0
    for user, posts in by_user.items():
        if user == "kan_news":
            continue
        finals = sorted(obs[-1][2] for _pid, obs in posts if obs[-1][1] >= MATURE_FINAL_H)
        if len(finals) < MIN_FINALS:
            continue
        for pid, obs in posts:
            if obs[-1][1] < MATURE_FINAL_H:
                continue
            pct = sum(1 for v in finals if v <= obs[-1][2]) / len(finals)
            best = 0.0
            for _pulled, age, eng, _u in obs:
                if age >= C.NOW_MAX_AGE_H:
                    continue
                base = C.baseline_at(by_user, user, pid, age)
                med = C.A._median(base) if len(base) >= C.NOW_MIN_BASE else 0
                if not med:
                    unscored += 1
                    continue
                best = max(best, eng / med)
            if best:
                peaks.append((best, pct, user))

    print(f"history rows {len(rows)}, posts scored {len(peaks)}, early observations without a baseline {unscored}\n")
    print(f"{'threshold':>9} {'fired':>6} {'median final pct':>17} {'≥p90':>6} {'<p50':>6}")
    for t in THRESHOLDS:
        fired = sorted(p for r, p, _u in peaks if r >= t)
        if not fired:
            print(f"{t:>9} {0:>6}")
            continue
        med = fired[len(fired) // 2]
        hi = sum(1 for p in fired if p >= 0.9) / len(fired)
        lo = sum(1 for p in fired if p < 0.5) / len(fired)
        print(f"{t:>9} {len(fired):>6} {med:>17.2f} {hi:>6.0%} {lo:>6.0%}")
    print("\nper account at the current threshold:")
    for user in sorted({u for _r, _p, u in peaks}):
        mine = [p for r, p, u in peaks if u == user and r >= C.NOW_MIN_RATIO]
        print(f"  {user:15} fired {len(mine):3}" + (f"  median final pct {sorted(mine)[len(mine) // 2]:.2f}" if mine else ""))


if __name__ == "__main__":
    main()
```

Run: `cd social_dashboard && venv/Scripts/python.exe -c "import ast; ast.parse(open('analyze_now_thresholds.py', encoding='utf-8').read()); print('ok')"`.
אל תריץ את הסקריפט עצמו מול הגיליון בשלב הזה: עוד אין שבוע של היסטוריה.

- [ ] **Step 2: מסמכים**

ב־`docs/ROADMAP.md`, בפריט 19, החלף את השורה שמתחילה ב־`- **3** —` ואת השורה שאחריה ב:

```markdown
- **3** — built: "now at rivals" (pace vs the account's own posts at the same
  age), engagement/1K from posts at ~24h (Kan included: the intraday run logs
  Kan's IG counts to the history), count trends in the account modal. The "now"
  section is **off** (`NOW_CALIBRATED = False`) until
  `social_dashboard/analyze_now_thresholds.py` is run on a week of history
  (from 2026-10-04) and the threshold is set by precision at maturity, not by
  firing rate.
```

ב־`docs/superpowers/specs/2026-09-27-competitors-design.md`, בסוף §3.2, הוסף:

```markdown
- **פתיחה אחרי כיול בלבד** (החלטת בן, 27.9): הספים שלמעלה הם נקודת התחלה. `NOW_CALIBRATED` נשאר False עד שמריצים `analyze_now_thresholds.py` על שבוע של היסטוריה, ובודקים באיזה אחוזון של החשבון נחת בבגרותו כל פוסט שהיה נדלק.
- **כאן בהיסטוריה:** הריצה התוך־יומית רושמת גם את ספירות האינסטגרם של כאן (`username = kan_news`), כדי שמעורבות/1K של כאן תימדד בגיל 24 שעות כמו של המתחרים. כאן לא מופיע ב"עכשיו אצל המתחרים".
```

- [ ] **Step 3: Commit**

```bash
git add social_dashboard/analyze_now_thresholds.py docs/ROADMAP.md docs/superpowers/specs/2026-09-27-competitors-design.md
git commit -m "competitors: calibration script for now-at-rivals, and docs for stage 3"
```
