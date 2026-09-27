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

import json
from datetime import date, datetime, timedelta

import aggregate as A

NEW_ACCOUNT_SLACK_DAYS = 1   # יום אחד בולע ריצה שנכשלה; יותר מזה = "חדש במעקב",
                             # אחרת חשבון בן 4 ימים נמדד מול 7 ימים של האחרים

ARENA_TOP = 12
# הדירוג מוחלט, אז החשבון הגדול ממלא הכול: ב־27.9 היו 11 מ־12 המקומות של N12
ARENA_PER_ACCOUNT = 2

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

POSTS_PER_ACCOUNT = 15
FEED_KEEP_DAYS = 14       # כמו POSTS_RETENTION_DAYS באספן

# יומן הריצה התוך־יומית ("מועמדי פערים"): שורת סימון לכל ריצה ושורה לכל סיפור
RUN_MARKER = "run"
CANDIDATE_COLUMNS = ["run_at", "kind", "cluster_key", "n_outlets", "posted_at", "caption",
                     "lead_username", "lead_eng", "lead_median", "lead_threshold",
                     "total_eng", "outlets", "best_kan_shared", "best_kan_containment",
                     "sources_checked"]

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


# ---------- פוסטים ----------

def post_view(p, cap_col, username, name, is_kan):
    likes, comments = A._int(p.get("likes")), A._int(p.get("comments"))
    return {"username": username, "name": name, "is_kan": is_kan,
            "date": str(A._parse_date(p.get("date")) or ""),
            "time": str(p.get("time", ""))[:5],
            "type": p.get("type", ""),
            "caption": A._clean_caption(p.get(cap_col, ""))[:200],
            "likes": likes, "comments": comments, "eng": likes + comments,
            "post_id": str(p.get("post_id") or p.get("media_id") or ""),
            "url": p.get("permalink", "")}


def arena(comp_posts, kan_posts, names, today):
    """שלשום ואתמול, מכל הפוסטים בחלון — סינון לפני מיון וחיתוך, עד ARENA_PER_ACCOUNT לחשבון."""
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
    picked, per = [], {}
    for it in items:
        if per.get(it["username"], 0) < ARENA_PER_ACCOUNT:
            per[it["username"]] = per.get(it["username"], 0) + 1
            picked.append(it)
            if len(picked) == ARENA_TOP:
                break
    return {"dates": [str(days[0]), str(days[1])], "posts": picked}


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


def gaps_from_log(rows, after, now):
    """הריצה התוך־יומית האחרונה, באותה צורה של coverage_gaps. None אם אין
    ריצה כזו — אז העמוד מחשב מנתוני הבוקר, שבהם המתחרים וכאן נכונים לאותה
    שעה. בלי זה, ספירות מתחרים של 14:00 מול פוסטים שלנו מהבוקר היו מסמנות
    כפער כל סיפור שפרסמנו מאז הבוקר.

    "היום" לפי תאריך קלנדרי אינו מספיק: אחרי חצות, ריצת 23:05 שייכת ל"אתמול"
    ונשמטת, והעמוד היה נופל בחזרה ל-coverage_gaps וזוגג פוסטי מתחרים מ-23:05
    מול לשוניות כאן מ-08:30 הקודם. לכן הקריטריון הוא עדכניות: הריצה חייבת
    להיות אחרי המשיכה של הבוקר (`after`, מ-freshness) וגם בתוך 24 השעות
    האחרונות (`now`), לא לפי "אותו תאריך קלנדרי"."""
    cutoff = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M")
    runs = [str(r.get("run_at", "")) for r in rows
            if r.get("kind") == RUN_MARKER and str(r.get("run_at", "")) > str(after)
            and str(r.get("run_at", "")) >= cutoff]
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
    # כאן checked סופר סיפורים (אשכולות), בעוד ב-coverage_gaps הוא סופר פוסטי
    # מועמדים לפני האיחוד לאשכולות — שני דברים שונים באותו שם שדה.
    return {"status": "ok", "failed": [], "stale": [], "source": "intraday", "run_at": last,
            "checked": len(missed) + len(exclusive),
            "missed": missed[:GAPS_TOP], "exclusive": exclusive[:GAPS_TOP]}


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


# ---------- העמוד ----------

def freshness(by_user, comp_posts=()):
    if not by_user:
        return None
    last = max(e[-1][0] for e in by_user.values())
    known = [u for u, e in by_user.items() if (last - e[-1][0]).days <= 7]
    today_rows = [e[-1][1] for e in by_user.values() if e[-1][0] == last]
    return {
        "date": str(last),
        "pulled_at": max((str(r.get("pulled_at", "")) for r in today_rows), default=""),
        "posts_pulled_at": max((str(p.get("pulled_at", "")) for p in comp_posts), default=""),
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
    if hasattr(data, "source_status"):
        for k in GAP_SOURCES:
            data.get(k)
        sources = data.source_status()
    else:
        sources = {}

    by_user = snapshots_by_user(data.get("competitors", []) or [])
    comp_posts = data.get("competitor_posts", []) or []
    ig = data.get("instagram", []) or []
    followers = data.get("followers", []) or []
    win = comparison_window(by_user, days)
    fw = feed_window(comp_posts, days, today)

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

    posts_by_user = {}
    for p in comp_posts:
        posts_by_user.setdefault(str(p.get("username", "")).strip(), []).append(p)

    names, rows = {}, []
    for u, entries in by_user.items():
        latest = entries[-1][1]
        names[u] = latest.get("name") or u
        own = posts_by_user.get(u, [])
        eng, basis = eng_fields(u, A._int(latest.get("followers")),
                                round(A._num(latest.get("eng_per_1k")), 2))
        rows.append({
            "username": u, "name": names[u], "is_kan": False,
            "followers": A._int(latest.get("followers")),
            "as_of": str(entries[-1][0]),
            "change_1d": A._int(latest.get("followers_change")),
            "growth": growth(entries, win),
            "posts_per_day": posts_per_day(own, fw),
            "eng_per_1k": eng, "eng_basis": basis,
            "spark": [A._int(r.get("followers")) for d, r in entries if win and d >= win["base"]],
            "posts": [with_trend(v) for v in sorted(
                (post_view(p, "caption", u, names[u], False) for p in own),
                key=lambda x: -x["eng"])[:POSTS_PER_ACCOUNT]],
        })

    # כאן — מהנתונים המלאים שלנו, באותם תאריכים בדיוק
    kan = kan_entries(followers)
    kan_followers = kan[-1][1]["followers"] if kan else 0
    recent = sorted((p for p in ig if A._parse_date(p.get("date"))),
                    key=lambda p: (str(p.get("date")), str(p.get("time", ""))), reverse=True)[:10]
    avg = sum(_eng(p) for p in recent) / len(recent) if recent else 0
    keep_from = today - timedelta(days=FEED_KEEP_DAYS)
    kan_eng, kan_basis = eng_fields("kan_news", kan_followers,
                                    round(avg / kan_followers * 1000, 2) if kan_followers else 0)
    rows.append({
        "username": "kan_news", "name": "כאן חדשות", "is_kan": True,
        "followers": kan_followers,
        "as_of": str(kan[-1][0]) if kan else None,
        "change_1d": A._int(followers[-1].get("ig_followers_change")) if followers else 0,
        "growth": growth(kan, win),
        "posts_per_day": posts_per_day(ig, fw),
        "eng_per_1k": kan_eng, "eng_basis": kan_basis,
        "spark": [r["followers"] for d, r in kan if win and d >= win["base"]],
        "posts": [with_trend(v) for v in sorted(
            (post_view(p, "caption", "kan_news", "כאן חדשות", True) for p in ig
             if (A._parse_date(p.get("date")) or keep_from) > keep_from),
            key=lambda x: -x["eng"])[:POSTS_PER_ACCOUNT]],
    })

    rows.sort(key=lambda c: -c["followers"])
    kan_rank = next(i + 1 for i, c in enumerate(rows) if c["is_kan"])
    fresh = freshness(by_user, comp_posts)
    after = (fresh or {}).get("pulled_at", "")
    return {
        "range": days,
        "last_date": A._last_data_date(data),
        "window": {k: _iso(v) for k, v in win.items()} if win else None,
        "feed": {k: _iso(v) for k, v in fw.items()} if fw else None,
        "freshness": fresh,
        "sources": sources,
        "summary": {"kan_rank": kan_rank, "ranked": len(rows),
                    "kan_growth": rows[kan_rank - 1]["growth"]},
        "arena": arena(comp_posts, ig, names, today),
        "now": now_at_rivals(hist, comp_posts, names, now),
        "gaps": (gaps_from_log(data.get("gap_candidates", []) or [], after, now)
                 or coverage_gaps(data, now, sources)),
        "competitors": rows,
    }
