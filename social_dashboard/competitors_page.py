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
