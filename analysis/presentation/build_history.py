#!/usr/bin/env python3
"""בונה היסטוריה חודשית אחידה מהייצואים של Business Suite — 2024 עד היום.

READ-ONLY. קורא רק קבצים מקומיים מ-`analysis/yearly_content/`, לא נוגע
בגיליונות ולא ב-API.

    python analysis/presentation/build_history.py

פלט: `history_monthly.csv` — שורה לכל (פלטפורמה, חודש) עם הספירות והמדדים,
ועמודות תקפות שמסמנות מאיזה חודש כל מדד אמין.

## למה יש עמודות תקפות

מטא שינתה את הגדרת החשיפה והצפיות באוגוסט–ספטמבר 2024, והייצוא לא שומר מדדים
ברמת פוסט מעבר ל~23 חודשים. נמדד ב-2026-08-11 על הייצואים עצמם:

* `Views` באינסטגרם ריק (NaN) לפני יולי 2024.
* `Reach` באינסטגרם לפני ספטמבר 2024 הוא זבל — חציון 3–24 על פוסטים עם
  1,500 לייקים ומעלה.
* בפייסבוק `Reach from Organic posts` שווה ל-`Reach` עד אוגוסט 2024 ואז קופץ
  לפי 2 ממנו, כשה-Boosted אפס לאורך כל השנה. אורגני לא יכול לעלות על הכל,
  ולכן העמודות החליפו משמעות.

ספירות, לייקים ותגובות יושבים על האובייקט ולא על ה-insights, ולכן שורדים את
כל 2024. כל גרף של צפיות/חשיפה חייב להתחיל בספטמבר 2024.
"""

import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, '..', 'yearly_content')
BS = os.path.join(SRC, 'data_buisness_suit')

# העמוד של כאן חדשות בייצוא הפייסבוק מזוהה ב-Page ID בפורמט הארוך, לא ב-220634478361516
FB_PAGE_ID = '100064467291406'
IG_USERNAME = 'kan_news'

# מאיזה חודש המדד אמין, לכל פלטפורמה בנפרד — ראו התיעוד למעלה.
# בפייסבוק ה-`Views` בייצוא 2024 ריק עד ספטמבר, כמו החשיפה באינסטגרם.
VALID_FROM = {
    'instagram': {'views': '2024-07', 'reach': '2024-09', 'shares': '2024-09',
                  'saves': '2024-09', 'follows': '2024-09'},
    'facebook': {'views': '2024-09', 'reach': '2024-09'},
}

IG_TYPES = {'IG reel': 'רילס', 'IG image': 'תמונה',
            'IG carousel': 'קרוסלה', 'IGTV': 'רילס'}

# בייצוא הגולמי `Video` נעלם אחרי אמצע 2025 ו-`Reel` מופיע במקומו — שינוי של
# מטא (כל וידאו מתפרסם כרילס מאז), לא שינוי בעמוד. לכן מאוחדים.
FB_TYPES = {'Photos': 'תמונה', 'Videos': 'וידאו', 'Reels': 'וידאו',
            'Text': 'סטטוס', 'Links': 'לינק'}

METRICS = ['views', 'reach', 'likes', 'comments', 'shares', 'saves', 'follows']


def _read(path):
    if path.endswith('.xlsx'):
        return pd.read_excel(path)
    return pd.read_csv(path, encoding='utf-8-sig', low_memory=False)


def _num(s):
    return pd.to_numeric(s, errors='coerce')


def load_instagram():
    """שלושת ייצואי האינסטגרם — 2024, 2025, 2026 YTD — לסכימה אחת."""
    files = [
        os.path.join(SRC, 'Jan-01-2024_Dec-31-2024_4588672571364735.csv'),
        os.path.join(BS, '2025 חוץ מסטוריז.xlsx'),
        os.path.join(BS, 'Jan-01-2026_Aug-10-2026_1071135865260092.csv'),
    ]
    out = []
    for f in files:
        d = _read(f)
        d = d[d['Account username'] == IG_USERNAME].copy()
        d['ts'] = pd.to_datetime(d['Publish time'], format='mixed', errors='coerce')
        d['format'] = d['Post type'].map(IG_TYPES)
        for src, dst in [('Views', 'views'), ('Reach', 'reach'), ('Likes', 'likes'),
                         ('Comments', 'comments'), ('Shares', 'shares'),
                         ('Saves', 'saves'), ('Follows', 'follows')]:
            d[dst] = _num(d[src]) if src in d.columns else pd.NA
        out.append(d[['ts', 'format'] + METRICS])
    d = pd.concat(out, ignore_index=True)
    d['platform'] = 'instagram'
    return d


def _fb_export(path):
    """ייצוא Business Suite של עמוד פייסבוק — 2024 ו-2026."""
    d = _read(path)
    d = d[d['Page ID'].astype(str) == FB_PAGE_ID].copy()
    d['ts'] = pd.to_datetime(d['Publish time'], format='mixed', errors='coerce')
    d['format'] = d['Post type'].map(FB_TYPES)
    d['views'] = _num(d['Views']) if 'Views' in d.columns else pd.NA
    d['watch_hours'] = _num(d['Seconds viewed']) / 3600
    d['reach'] = _num(d['Reach'])
    d['likes'] = _num(d['Reactions'])
    d['comments'] = _num(d['Comments'])
    d['shares'] = _num(d['Shares'])
    d['saves'] = d['follows'] = pd.NA
    return d


def _fb_backfill(path):
    """2025, שנמשך מה-Graph API כי הייצוא הידני נתקע (`fb_2025_backfill.py`).

    אין כאן `reach` בכוונה: `post_total_media_view_unique` נשחק עם גיל הפוסט
    ולכן לא נמשך. עמודה שנראית תקינה ואינה תקינה גרועה מעמודה חסרה.
    """
    d = _read(path)
    d['ts'] = pd.to_datetime(d['date'] + ' ' + d['time'].astype(str), errors='coerce')
    d['format'] = d['type'].map({'Photo': 'תמונה', 'Reel': 'וידאו', 'Video': 'וידאו',
                                 'Status': 'סטטוס', 'Link': 'לינק'})
    d['views'] = _num(d['views'])
    d['watch_hours'] = _num(d['watch_min']) / 60
    d['likes'] = _num(d['reactions'])
    d['comments'] = _num(d['comments'])
    d['shares'] = _num(d['shares'])
    d['reach'] = d['saves'] = d['follows'] = pd.NA
    return d


def load_facebook():
    """שלוש שנים משלושה מקורות — שניים מיוצאים ואחד נמשך.

    הגיליון `נתוני פייסבוק` **לא** משמש כאן: 38–53% מהשורות בו מחזיקות אפס
    צפיות מנובמבר 2025 ועד מאי 2026, והאפסים נעלמים ביוני 2026 — בדיוק
    כשמטא הסירה את `post_impressions_unique` (15.6.2026) והקולקטור עבר ל-v25.
    מול הייצוא של מרץ 2026 הגיליון קורא 2.44 פעמים נמוך מדי.
    """
    parts = [
        _fb_export(os.path.join(SRC, 'Jan-01-2024_Dec-31-2024_1509169891251840.csv')),
        _fb_backfill(os.path.join(HERE, 'pulled', 'fb_2025_metrics.csv')),
        _fb_export(os.path.join(SRC, 'Jan-01-2026_Aug-11-2026_1369894801784288.csv')),
    ]
    cols = ['ts', 'format', 'watch_hours'] + METRICS
    d = pd.concat([p[cols] for p in parts], ignore_index=True)
    d['platform'] = 'facebook'
    return d


def monthly(d):
    d = d[d['ts'].notna()].copy()
    d['month'] = d['ts'].dt.to_period('M').astype(str)
    cols = [m for m in METRICS if m in d.columns]
    cols += [c for c in ('video_views_3s', 'watch_hours') if c in d.columns]
    agg = {c: (c, 'sum') for c in cols}
    g = d.groupby(['platform', 'month']).agg(posts=('ts', 'size'), **agg).reset_index()

    # ספירת פורמטים כעמודות, כדי ששקף התמהיל יקרא שורה אחת לחודש
    piv = (d.pivot_table(index=['platform', 'month'], columns='format',
                         values='ts', aggfunc='size', fill_value=0).reset_index())
    g = g.merge(piv, on=['platform', 'month'], how='left')

    # מסמנים ולא מוחקים: הערך נשאר בקובץ, והדגל אומר אם מותר לצייר אותו
    for metric in METRICS:
        if metric not in g.columns:
            continue
        col = '%s_valid' % metric
        g[col] = True
        for platform, rules in VALID_FROM.items():
            if metric in rules:
                bad = (g['platform'] == platform) & (g['month'] < rules[metric])
                g.loc[bad, col] = False
    return g.sort_values(['platform', 'month'])


def main():
    ig = load_instagram()
    fb = load_facebook()
    both = pd.concat([ig, fb], ignore_index=True)
    g = monthly(both)

    out = os.path.join(HERE, 'history_monthly.csv')
    g.to_csv(out, index=False, encoding='utf-8-sig')

    print("נכתב: %s  (%d שורות)" % (out, len(g)))
    for p, sub in g.groupby('platform'):
        print("\n%s — %s עד %s, %s פריטים" % (
            p, sub['month'].min(), sub['month'].max(), format(int(sub['posts'].sum()), ',')))
        for metric in METRICS + ['watch_hours']:
            if metric not in sub.columns:
                continue
            col = '%s_valid' % metric
            ok = sub[sub[col]] if col in sub.columns else sub
            total = ok[metric].sum(skipna=True)
            if pd.isna(total) or total == 0:
                continue
            first = VALID_FROM.get(p, {}).get(metric)
            print("   %-12s %16s%s" % (
                metric, format(int(total), ','), '  (מ-%s)' % first if first else ''))
        # שנה מול שנה, כי זה מה שהמצגת בעצם שואלת
        for metric in ('views', 'watch_hours'):
            if metric not in sub.columns:
                continue
            col = '%s_valid' % metric
            ok = sub[sub[col]] if col in sub.columns else sub
            by_year = ok.groupby(ok['month'].str[:4])[metric].sum()
            parts = ["%s %s" % (y, format(int(v), ',')) for y, v in by_year.items() if v]
            if parts:
                print("     %-10s %s" % (metric, ' | '.join(parts)))


if __name__ == '__main__':
    sys.exit(main())
