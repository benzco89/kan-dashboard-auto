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
