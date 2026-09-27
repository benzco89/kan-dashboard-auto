"""כיול "עכשיו אצל המתחרים" - קריאה בלבד. מריצים אחרי שבוע של היסטוריה.

הקריטריון הוא דיוק מול אמת שאינה תלויה בטריגר, כמו בכיול הסניפר: פוסט שהיה
צעיר "פי X מהרגיל" - באיזה אחוזון של החשבון שלו הוא נחת בגיל קבוע? לא "כמה
פעמים זה נדלק". כדי שהמספר לא ייצא אופטימי:

  * האמת היא הספירה בגיל קבוע (33 שעות, ±4) - לא התצפית האחרונה, שגילה משתנה
    מפוסט לפוסט (פוסט שנמשך בגיל 70 היה נראה "גבוה" רק כי הוא ותיק).
  * האחוזון מחושב מול שאר הפוסטים של החשבון, בלי הפוסט עצמו.
  * פוסט "נדלק" בסף t בתצפית הצעירה הראשונה (גיל < 24) שבה היחס עבר את t - לא
    ביחס המקסימלי, שהעמוד לא היה רואה בזמן אמת. גיל הדליקה נרשם, כי הערך של
    החלק הוא זיהוי מוקדם (2-8 שעות).
  * חשבון עם פחות מ-MIN_FINALS פוסטים שיש להם אמת - לא נכנס, ומודפס עם הספירה.

    cd social_dashboard && venv/Scripts/python.exe analyze_now_thresholds.py

אחרי הכיול: לעדכן את NOW_MIN_RATIO (ואם צריך NOW_AGE_TOL_H / NOW_MIN_BASE)
ב-competitors_page.py, להעביר את NOW_CALIBRATED ל-True ולתעד את המספרים
ב-ROADMAP.
"""

import sys
from statistics import median

import competitors_page as C

TRUTH_AGE_H = 33           # "איפה הפוסט נחת": הספירה בגיל 33 שעות
TRUTH_TOL_H = 4            # ±4 שעות; בלי תצפית כזו הפוסט לא נכנס
MIN_FINALS = 10            # חשבון עם פחות פוסטים שיש להם אמת לא נכנס לכיול
THRESHOLDS = (1.5, 2.0, 2.5, 3.0)
AGE_BUCKETS = ((0, 4, "<4h"), (4, 8, "4-8h"), (8, 16, "8-16h"), (16, 24, "16-24h"))


def bucket(age):
    for lo, hi, label in AGE_BUCKETS:
        if lo <= age < hi:
            return label
    return None


def truth_obs(obs):
    """התצפית הקרובה ביותר לגיל 33 שעות, בתוך ±4; None אם אין."""
    return C._closest(obs, TRUTH_AGE_H, TRUTH_TOL_H)


def account_truths(posts):
    """{post_id: הספירה בגיל האמת} לפוסטים של חשבון אחד שיש להם תצפית כזו."""
    out = {}
    for pid, obs in posts:
        t = truth_obs(obs)
        if t:
            out[pid] = t[2]
    return out


def percentile(truths, pid):
    """מקום הפוסט בין שאר הפוסטים של החשבון (בלי עצמו), 0-1; תיקו נספר חצי."""
    own = truths[pid]
    others = [v for p, v in truths.items() if p != pid]
    if not others:
        return None
    below = sum(1 for v in others if v < own)
    ties = sum(1 for v in others if v == own)
    return (below + 0.5 * ties) / len(others)


def early_scores(obs, by_user, user, pid):
    """[(age, ratio או None, בסיס או None), ...] לכל תצפית צעירה, לפי סדר המשיכה.
    ratio None = לא היו NOW_MIN_BASE נקודות בסיס (או שהחציון 0)."""
    out = []
    for _pulled, age, eng, _u in obs:
        if age >= C.NOW_MAX_AGE_H:
            continue
        base = C.baseline_at(by_user, user, pid, age)
        med = C.A._median(base) if len(base) >= C.NOW_MIN_BASE else 0
        out.append((age, eng / med if med else None, med or None))
    return out


def first_fire(obs, by_user, user, pid, t, scores=None):
    """גיל התצפית הצעירה הראשונה שבה היחס ≥ t; None אם לא נדלק."""
    for age, ratio, _med in (scores if scores is not None else early_scores(obs, by_user, user, pid)):
        if ratio is not None and ratio >= t:
            return age
    return None


def _stats(pcts):
    if not pcts:
        return {"fired": 0, "median_pct": None, "share_p90": None, "share_below_p50": None}
    return {"fired": len(pcts), "median_pct": median(pcts),
            "share_p90": sum(1 for p in pcts if p >= 0.9) / len(pcts),
            "share_below_p50": sum(1 for p in pcts if p < 0.5) / len(pcts)}


def calibrate(hist):
    """כל המספרים של הכיול מהיסטוריה אחת (history_by_post), בלי גישה לגיליון."""
    by_user = C.posts_by_account(hist)
    included, excluded = {}, {}
    fires = {t: [] for t in THRESHOLDS}                   # (pct, age at first firing, user)
    base_obs = {label: {"with": 0, "without": 0, "bases": []} for _lo, _hi, label in AGE_BUCKETS}
    scored = 0
    for user, posts in sorted(by_user.items()):
        if user == "kan_news":
            continue
        truths = account_truths(posts)
        if len(truths) < MIN_FINALS:
            excluded[user] = len(truths)
            continue
        included[user] = len(truths)
        for pid, obs in posts:
            if pid not in truths:
                continue
            pct = percentile(truths, pid)
            if pct is None:
                continue
            scored += 1
            scores = early_scores(obs, by_user, user, pid)
            for age, ratio, med in scores:
                b = bucket(age)
                if b is None:
                    continue
                if ratio is None:
                    base_obs[b]["without"] += 1
                else:
                    base_obs[b]["with"] += 1
                    base_obs[b]["bases"].append(med)
            for t in THRESHOLDS:
                age = first_fire(obs, by_user, user, pid, t, scores)
                if age is not None:
                    fires[t].append((pct, age, user))

    out_t = {}
    for t, fired in fires.items():
        out_t[t] = {"overall": _stats([p for p, _a, _u in fired]),
                    "by_age": {label: _stats([p for p, a, _u in fired if bucket(a) == label])
                               for _lo, _hi, label in AGE_BUCKETS}}
    per_account = {}
    for user in included:
        mine = [p for p, _a, u in fires.get(C.NOW_MIN_RATIO, []) if u == user]
        per_account[user] = _stats(mine)
    return {
        "included": included, "excluded": excluded, "posts_scored": scored,
        "thresholds": out_t,
        "baselines": {label: {"with": v["with"], "without": v["without"],
                              "median_base": median(v["bases"]) if v["bases"] else None}
                      for label, v in base_obs.items()},
        "per_account": per_account,
    }


def _row(label, s):
    if not s["fired"]:
        return f"  {label:>8} {0:>6}"
    return (f"  {label:>8} {s['fired']:>6} {s['median_pct']:>10.2f} "
            f"{s['share_p90']:>6.0%} (base rate 10%) {s['share_below_p50']:>6.0%}")


def report(res):
    print(f"truth: the count at {TRUTH_AGE_H}h (±{TRUTH_TOL_H}h); percentile among the account's "
          f"other posts; accounts need ≥{MIN_FINALS} posts with a truth observation\n")
    print("accounts included (posts with truth): " +
          (", ".join(f"{u} {n}" for u, n in res["included"].items()) or "none"))
    print("accounts EXCLUDED (fewer than MIN_FINALS posts with truth):")
    for u, n in sorted(res["excluded"].items(), key=lambda e: -e[1]):
        print(f"  {u:20} {n}")
    print(f"\nposts scored {res['posts_scored']}\n")
    for t, r in res["thresholds"].items():
        print(f"threshold {t}  (fired at the FIRST early observation at or above it)")
        print(f"  {'age':>8} {'fired':>6} {'median pct':>10} {'≥p90':>6} {'':15} {'<p50':>6}")
        print(_row("all", r["overall"]))
        for label, s in r["by_age"].items():
            print(_row(label, s))
        print()
    print("early observations by age: with a baseline (≥NOW_MIN_BASE posts) / without, median baseline")
    for label, b in res["baselines"].items():
        mb = f"{b['median_base']:.0f}" if b["median_base"] is not None else "-"
        print(f"  {label:>8} with {b['with']:5}  without {b['without']:5}  median baseline {mb}")
    print(f"\nper account at the current threshold ({C.NOW_MIN_RATIO}):")
    for user, s in res["per_account"].items():
        print(f"  {user:20} fired {s['fired']:3}" +
              (f"  median final pct {s['median_pct']:.2f}" if s["fired"] else ""))


def main():
    import gsheets          # כאן ולא למעלה: הבדיקות מייבאות את הקובץ בלי גיליון
    sys.stdout.reconfigure(encoding="utf-8")
    rows = gsheets.get_data(keys=("competitor_history",)).get("competitor_history", [])
    print(f"history rows {len(rows)}")
    report(calibrate(C.history_by_post(rows)))


if __name__ == "__main__":
    main()
