#!/usr/bin/env python3
"""מה נמדד, לכל רשת ולכל מדד, ומאיזה תאריך — **נקרא מהקבצים, לא מהזיכרון.**

    python analysis/presentation/coverage.py

הטבלה ב-`PLAN.md` ובקבוע `COVERAGE` שב-`render_deck.py` נכתבו ביד, ויד שוכחת.
כשמדד אחד מתחיל בספטמבר 2024 ואחר בינואר, הפרש של שמונה חודשים נבלע בשקף
ונראה כמו צמיחה. הסקריפט הזה פותח את הקבצים ומדווח את התאריך הראשון והאחרון
שבהם באמת יש ערך — כדי שאפשר יהיה להצליב מול מה שהדק מציג.

**"יש ערך" הוא לא "יש שורה".** בפייסבוק 2024 יש שורה חודשית לכל חודש ובה
`views=0`, כי המדד לא היה קיים; לכן הספירה כאן היא של ערכים שאינם ריקים
ואינם אפס, ולפייסבוק ואינסטגרם גם נשענת על דגלי ה-`_valid` ש-`build_history`
כבר חישב.
"""
import csv
import os
import sys

import pandas as pd

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HERE = os.path.dirname(os.path.abspath(__file__))
PULLED = os.path.join(HERE, 'pulled')
SRC = os.path.join(HERE, '..', 'yearly_content')
STUDIO = os.path.join(SRC, 'youtube_studio')
META = os.path.join(SRC, 'meta_insights')

HEB = {'facebook': 'פייסבוק', 'instagram': 'אינסטגרם', 'youtube': 'יוטיוב',
       'tiktok': 'טיקטוק', 'twitter': 'X', 'whatsapp': 'ערוץ וואטסאפ'}

# סדר המדדים בדוח. המפתח הוא מה שמוצג, לא שם העמודה.
METRICS = ['פריטים', 'צפיות', 'חשיפה', 'מעורבות', 'עוקבים — מלאי',
           'עוקבים — תוספות']

rows = []          # (רשת, מדד, מאיפה, עד, נקודות, מקור)


def add(plat, metric, first, last, n, src):
    rows.append((plat, metric, str(first)[:10] if first else '—',
                 str(last)[:10] if last else '—', n, src))


def _span(series, dates):
    """התאריך הראשון והאחרון שבהם הערך אינו ריק ואינו אפס."""
    ok = series.notna() & (series != 0)
    if not ok.any():
        return None, None, 0
    d = pd.to_datetime(dates[ok], errors='coerce').dropna()
    return d.min(), d.max(), int(ok.sum())


def meta_daily(fn):
    """ייצוא Meta Insights: UTF-16, שורת `sep=,`, ובלוקים אחדים בקובץ אחד.

    לכן לא `read_csv` אלא סריקה לשורות של שני שדות שהראשון בהן הוא חותמת
    זמן — כל השאר בקובץ הוא כותרות בלוק וסיכומים.
    """
    out = []
    path = os.path.join(META, fn)
    if not os.path.exists(path):
        return pd.DataFrame(columns=['date', 'v'])
    with open(path, encoding='utf-16') as fh:
        for r in csv.reader(fh):
            if len(r) == 2 and r[0][:4].isdigit() and 'T' in r[0]:
                try:
                    out.append((r[0][:10], float(r[1])))
                except ValueError:
                    pass
    return pd.DataFrame(out, columns=['date', 'v'])


def main():
    # ---------- פייסבוק ואינסטגרם: הבסיס החודשי, לפי דגלי התקפות ----------
    m = pd.read_csv(os.path.join(HERE, 'history_monthly.csv'), encoding='utf-8-sig')
    pairs = [('פריטים', 'posts', None), ('צפיות', 'views', 'views_valid'),
             ('חשיפה', 'reach', 'reach_valid'), ('מעורבות', 'likes', 'likes_valid'),
             ('עוקבים — תוספות', 'follows', 'follows_valid')]
    for plat in ('facebook', 'instagram'):
        sub = m[m['platform'] == plat]
        for label, col, flag in pairs:
            s = sub[sub[flag]] if flag else sub
            f, l, n = _span(s[col], s['month'])
            add(plat, label, f, l, n, 'history_monthly.csv')

    # ---------- עוקבים: מלאי, מהגיליון היומי ----------
    fol = pd.read_csv(os.path.join(PULLED, 'sheet_followers.csv'), encoding='utf-8-sig')
    for plat, col in (('youtube', 'yt_subscribers'), ('facebook', 'fb_followers'),
                      ('instagram', 'ig_followers'), ('tiktok', 'tt_followers'),
                      ('twitter', 'tw_followers')):
        if col not in fol.columns:
            add(plat, 'עוקבים — מלאי', None, None, 0, 'אין עמודה בגיליון')
            continue
        f, l, n = _span(pd.to_numeric(fol[col], errors='coerce'), fol['date'])
        add(plat, 'עוקבים — מלאי', f, l, n, 'sheet_followers.csv')

    # יוטיוב הוא היחיד עם מלאי יומי מדוד לכל התקופה — מ-Studio, לא מהגיליון.
    sub = pd.read_csv(os.path.join(STUDIO, 'subscribers_daily.csv'))
    f, l, n = _span(pd.to_numeric(sub['Subscribers'], errors='coerce'), sub['Date'])
    add('youtube', 'עוקבים — מלאי', f, l, n, 'youtube_studio/subscribers_daily.csv')

    # ---------- תוספות עוקבים יומיות, מייצוא מטא ----------
    for plat, fn in (('facebook', 'Follows.csv'),
                     ('instagram', 'Audience_instagram.csv')):
        d = meta_daily(fn)
        if len(d):
            f, l, n = _span(d['v'], d['date'])
            add(plat, 'עוקבים — תוספות', f, l, n, 'meta_insights/' + fn)

    # ---------- יוטיוב וטיקטוק: פריטים ----------
    for plat, fn in (('youtube', 'youtube_history.csv'),
                     ('tiktok', 'tiktok_history.csv')):
        d = pd.read_csv(os.path.join(PULLED, fn), encoding='utf-8-sig', low_memory=False)
        dt = pd.to_datetime(d['date'], errors='coerce')
        add(plat, 'פריטים', dt.min(), dt.max(), int(dt.notna().sum()), fn)
        f, l, n = _span(pd.to_numeric(d['views'], errors='coerce'), d['date'])
        add(plat, 'צפיות', f, l, n, fn + ' (מצטבר לפריט)')
        f, l, n = _span(pd.to_numeric(d['likes'], errors='coerce'), d['date'])
        add(plat, 'מעורבות', f, l, n, fn)

    # יוטיוב: צפיות **שהתרחשו** ביום, להבדיל ממה שפריט צבר לאורך חייו
    dv = pd.read_csv(os.path.join(STUDIO, 'daily_views.csv'))
    f, l, n = _span(pd.to_numeric(dv['Views'], errors='coerce'), dv['Date'])
    add('youtube', 'צפיות', f, l, n, 'youtube_studio/daily_views.csv (ביום)')

    # ---------- X ----------
    tw = pd.read_csv(os.path.join(PULLED, 'sheet_twitter.csv'), encoding='utf-8-sig')
    dt = pd.to_datetime(tw['date'], errors='coerce')
    add('twitter', 'פריטים', dt.min(), dt.max(), int(dt.notna().sum()),
        'sheet_twitter.csv')
    f, l, n = _span(pd.to_numeric(tw['views'], errors='coerce'), tw['date'])
    add('twitter', 'צפיות', f, l, n, 'sheet_twitter.csv')

    # ---------- ערוץ וואטסאפ ----------
    for metric in METRICS:
        add('whatsapp', metric, None, None, 0, 'אין API כלל')

    # ---------- הדפסה ----------
    order = {p: i for i, p in enumerate(
        ['facebook', 'tiktok', 'youtube', 'twitter', 'whatsapp', 'instagram'])}
    rows.sort(key=lambda r: (order.get(r[0], 9), METRICS.index(r[1])
                             if r[1] in METRICS else 9))
    print('%-11s %-17s %-11s %-11s %8s  %s'
          % ('רשת', 'מדד', 'מאיפה', 'עד', 'נקודות', 'מקור'))
    print('-' * 108)
    last = None
    for plat, metric, f, l, n, src in rows:
        if last and plat != last:
            print()
        last = plat
        print('%-11s %-17s %-11s %-11s %8s  %s'
              % (HEB.get(plat, plat), metric, f, l, format(n, ','), src))


if __name__ == '__main__':
    main()
