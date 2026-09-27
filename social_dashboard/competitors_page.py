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

ARENA_TOP = 12

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
