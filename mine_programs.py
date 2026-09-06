# -*- coding: utf-8 -*-
"""גוזר את PROGRAM_BY_PERSON מהנתונים. READ ONLY - מדפיס, לא כותב לקוד.

**הרעיון:** התוכנית כמעט אף פעם לא כתובה בכיתוב. חתימה בסוגריים ו-@mention
נותנים *מי*, וההאשטאג נותן *איזו תוכנית* - אבל ההאשטאג נדיר (9% מהפריטים).
פריט שנושא את שניהם הוא לכן **דוגמה מתויגת**, ומאלה אפשר לגזור את הקישור
שחסר בכל השאר. 198 פריטים כאלה מתוך 2,564 כיתובים הספיקו כדי להכפיל את
הכיסוי (11.9% -> 18.1%, נמדד 2026-09-06).

**הסף אינו קוסמטי.** לא לכל כתב יש תוכנית אחת: אליאור לוי יצא 4/4/5 בין
חדשות הלילה, בשכונה שלנו ושובר חומות, ושיוך שלו לאחת מהן הוא המצאה ולא
מדידה. MIN_SHARE הוא מה שמפריד בין השניים, ומי שנופל ממנו נשאר עם person
בלבד - מצב קריא, לא שגיאה.

**מה שהוא לא עושה:** לא כותב לקובץ ולא דורס. הפלט מיועד להשוואה עם הטבלה
הקיימת ולהחלטה אנושית, כי שורה שאדם קבע (כרמלה מנשה -> רשת ב׳) גוברת על
מה שהנתונים מראים.

    python mine_programs.py            # מהגיליונות + ig_captions/ אם קיים

Env: GCP_SERVICE_ACCOUNT או service-account.json בתיקייה.
"""

import collections
import csv
import glob
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "social_dashboard"))
import content_tags as ct  # noqa: E402

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
MIN_N = 3
MIN_SHARE = 0.70


def load_captions():
    caps = []
    for path in sorted(glob.glob("ig_captions/*.csv")):
        with open(path, encoding="utf-8-sig") as fh:
            for r in csv.DictReader(fh):
                caps.append(r.get("caption_full") or "")
        print(f"  {path}: {len(caps)} עד כה")

    creds_json = (os.environ.get("GCP_SERVICE_ACCOUNT")
                  or os.environ.get("GOOGLE_CREDENTIALS"))
    if not creds_json and os.path.exists("service-account.json"):
        creds_json = open("service-account.json", encoding="utf-8").read()
    if not creds_json:
        print("  אין אישורי גיליונות - ממשיך עם ig_captions/ בלבד")
        return caps

    import gspread
    from google.oauth2.service_account import Credentials
    creds = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    sh = gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    for name, col, only_reel in (("נתוני טיקטוק", "title", False),
                                 ("נתוני אינסטגרם", "caption", True)):
        try:
            rows = sh.worksheet(name).get_all_records()
        except Exception as e:
            print(f"  {name}: {str(e)[:60]}")
            continue
        n = 0
        for r in rows:
            if only_reel and str(r.get("type", "")).lower() != "reel":
                continue
            caps.append(str(r.get(col) or ""))
            n += 1
        print(f"  {name}: +{n}")
    return caps


def people_in(caption):
    """כל מי שהכיתוב מזהה - חתימה בסוגריים ואזכורים שממופים לאדם."""
    out = set(ct.extract_byline(caption) or [])
    out |= {ct.HANDLE_TO_PERSON[h] for h in ct.extract_handles(caption)
            if h in ct.HANDLE_TO_PERSON}
    return out


def program_in(caption):
    for tag in ct.extract_hashtags(caption):
        if tag in ct.PROGRAM_BY_HASHTAG:
            return ct.PROGRAM_BY_HASHTAG[tag]
    return None


def coverage(captions, table):
    """כמה כיתובים מקבלים תוכנית - בהאשטאג או דרך הטבלה."""
    hit = 0
    for cap in captions:
        if not cap:
            continue
        if program_in(cap) or any(p in table for p in people_in(cap)):
            hit += 1
    return hit


def main():
    print("=" * 62)
    print("כריית PROGRAM_BY_PERSON · READ ONLY")
    print("=" * 62)
    caps = load_captions()
    print(f"\nקורפוס: {len(caps)} כיתובים")

    pairs = collections.defaultdict(collections.Counter)
    labelled = 0
    for cap in caps:
        if not cap:
            continue
        prog = program_in(cap)
        if not prog:
            continue
        labelled += 1
        for person in people_in(cap):
            pairs[person][prog] += 1
    print(f"נושאים גם האשטאג וגם שם: {labelled}\n")

    accept, split = {}, []
    for person, counts in pairs.items():
        total = sum(counts.values())
        prog, n = counts.most_common(1)[0]
        if n >= MIN_N and n / total >= MIN_SHARE:
            accept[person] = (prog, n, total)
        elif total >= MIN_N:
            split.append((person, dict(counts)))

    print(f"עוברים את הסף ({MIN_N}+ מופעים, {int(MIN_SHARE * 100)}%+ דומיננטיות):")
    for person, (prog, n, total) in sorted(
            accept.items(), key=lambda kv: -kv[1][2]):
        cur = ct.PROGRAM_BY_PERSON.get(person)
        mark = ("כבר בטבלה" if cur == prog else
                f"⚠️ בטבלה: {cur}" if cur else "חדש")
        print(f"   {person:20s} → {prog:16s} {n}/{total:<4d} {mark}")

    if split:
        print(f"\nמפוצלים בין תוכניות - לא משויכים בכוונה ({len(split)}):")
        for person, counts in split:
            print(f"   {person:20s} {counts}")

    now = coverage(caps, ct.PROGRAM_BY_PERSON)
    after = coverage(caps, {**ct.PROGRAM_BY_PERSON,
                            **{p: v[0] for p, v in accept.items()}})
    print(f"\nכיסוי program: {now} ({now * 100 / max(len(caps), 1):.1f}%) "
          f"→ {after} ({after * 100 / max(len(caps), 1):.1f}%)")

    new = {p: v[0] for p, v in accept.items()
           if ct.PROGRAM_BY_PERSON.get(p) != v[0]}
    if new:
        print("\nלהעתקה ל-PROGRAM_BY_PERSON (בדקו סתירות לפני):")
        for person, prog in new.items():
            print(f'    "{person}": "{prog}",')
    else:
        print("\nהטבלה מעודכנת - אין מה להוסיף.")


if __name__ == "__main__":
    main()
