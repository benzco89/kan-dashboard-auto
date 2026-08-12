#!/usr/bin/env python3
"""מחשב את כל מספרי המצגת 2024→היום ל-`deck_content.json`.

    python analysis/presentation/build_deck.py

הפרדה מכוונת בין החישוב לבין הניסוח, כמו ב-`weekly_deck`: הסקריפט מחשב מספרים
ולא כותב משפטים. הכותרות, המסקנות והמשפט של כל שקף יושבים ב-JSON תחת
`editorial`, נכתבים ביד, ו**שורדים הרצה חוזרת** — ראו `_merge_editorial`.
מצגת להנהלה נופלת או עומדת על הניסוח, וזה לא משהו שסקריפט צריך להמציא.

## היחידה שכל המצגת מדברת בה

"צפיות שנצברו לתוכן שפורסם בחודש X" — לא "צפיות בחודש X". בכל ארבע הרשתות
המדד הוא מצטבר-עד-היום ברמת הפריט, ולכן הוא עקבי בין הפלטפורמות אבל מוטה
לטובת תוכן ותיק שהספיק לצבור. ההטיה קטנה בפייסבוק/אינסטגרם/טיקטוק — פוסט גמור
תוך ארבעה ימים (ROADMAP, שורה סגורה 27.7.2026) — וגדולה יותר ביוטיוב, שיש לו
זנב אמיתי. נאמר על השקף ולא מוסתר.

## מה לא נכנס לכאן, ולמה

* **חשיפה בפייסבוק 2025** — לא נמשכה (המדד נשחק עם גיל הפוסט).
* **טוויטר לפני 6/2026** — הספק מגיע 13 יום אחורה בלבד.
* **הגיליון `נתוני פייסבוק`** — 38–53% מהשורות עם אפס צפיות לפני יוני 2026.
* **צופים קבועים ביוטיוב** — מדד שהושק ב-20.3.2025.
"""

import csv
import json
import os
import sys
from collections import defaultdict

import pandas as pd

# הקונסולה של Windows היא cp1255 ונופלת על חץ יוניקוד באמצע דוח תקין
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, '..', 'yearly_content')
PULLED = os.path.join(HERE, 'pulled')
OUT = os.path.join(HERE, 'deck_content.json')

TODAY = '2026-08-11'
# הכל נחתך בסוף יולי 2026. אוגוסט הוא חודש חלקי, ובכל גרף חודשי הוא מצייר
# צניחה שהיא רק «החודש עוד לא נגמר» — הקורא רואה נפילה שלא קרתה.
CUTOFF = '2026-07-31'
CUTOFF_MONTH = '2026-07'
YTD_CUT = (7, 31)          # התקופה המקבילה: 1 בינואר עד 31 ביולי
VIEWS_FROM = '2024-09'     # מטא הגדירה מחדש צפיות/חשיפה באוג'–ספט' 2024

PLATFORMS = ['facebook', 'youtube', 'tiktok', 'instagram', 'twitter', 'whatsapp']

# עוקבים נכון ל-2026-08-11 (`מעקב עוקבים`); וואטסאפ נמסר ידנית — אין לו API
FOLLOWERS = {
    'facebook': 1183166, 'tiktok': 828200, 'youtube': 798000,
    'twitter': 373889, 'whatsapp': 310905, 'instagram': 283818,
}


def _num(s):
    return pd.to_numeric(s, errors='coerce')


def clean_title(s, limit=88):
    """כותרת לשקף: בלי סימני bidi, בגבול מילה, ובלי שובל של תיוג חתוך.

    שלוש מלכודות שחוזרות בכל פעם שטקסט מהרשתות מגיע לתצוגה עברית:
    הקאפשנים מכילים תווי כיווניות בלתי נראים (U+2066-2069, RLM/LRM) שמזיזים
    חצי שורה; חיתוך לפי מספר תווים חותך באמצע מילה; ותיוג לטיני בסוף
    (`@itayblumental`) נחתך מהצד הלא נכון ומשאיר `itayblumental@` תלוי באוויר.
    """
    s = str(s or '')
    for ch in ('⁦', '⁧', '⁨', '⁩', '‎', '‏', '‪',
               '‫', '‬', '­'):
        s = s.replace(ch, '')
    s = ' '.join(s.split())                      # טאבים וירידות שורה
    if len(s) > limit:
        cut = s[:limit]
        if ' ' in cut:
            cut = cut[:cut.rfind(' ')]
        s = cut.rstrip(' ,.;:-–—') + '…'
    # תיוג שנשאר חתוך בקצה — להוריד אותו ולא להציג חצי שם
    words = s.split(' ')
    while words and (words[-1].startswith('@') or words[-1].endswith('@')):
        words.pop()
    return ' '.join(words).rstrip(' ,.;:-–—…') or s


def _ytd(d, col='dt'):
    """אותו חלון בכל שנה, כדי ש-2026 לא תושווה לשנה שלמה."""
    m, day = YTD_CUT
    return d[(d[col].dt.month < m) | ((d[col].dt.month == m) & (d[col].dt.day <= day))]


def load_meta():
    """פייסבוק ואינסטגרם — מהבסיס החודשי ש-build_history בנה."""
    p = os.path.join(HERE, 'history_monthly.csv')
    if not os.path.exists(p):
        raise SystemExit("❌ חסר history_monthly.csv — להריץ קודם build_history.py")
    return pd.read_csv(p, encoding='utf-8-sig')


def load_video(name):
    """יוטיוב/טיקטוק — שורה לפריט, עם צפיות מצטברות."""
    d = pd.read_csv(os.path.join(PULLED, name), encoding='utf-8-sig')
    d['dt'] = pd.to_datetime(d['date'], errors='coerce')
    d = d.dropna(subset=['dt'])
    d = d[d['dt'] <= CUTOFF]
    for c in ('views', 'likes', 'comments'):
        if c in d.columns:
            d[c] = _num(d[c]).fillna(0)
    return d


def yearly_from_video(d):
    out = {}
    for y, g in d.groupby(d['dt'].dt.year):
        out[str(y)] = {'posts': len(g), 'views': int(g['views'].sum()),
                       'likes': int(g['likes'].sum())}
    ytd = {}
    for y, g in _ytd(d).groupby(d['dt'].dt.year):
        ytd[str(y)] = {'posts': len(g), 'views': int(g['views'].sum())}
    return out, ytd


def monthly_from_video(d, since='2024-09'):
    s = d[d['dt'] >= since].groupby(d['dt'].dt.to_period('M'))['views'].sum()
    return [{'month': str(m), 'views': int(v)} for m, v in s.items()]


def build():
    meta = load_meta()
    yt = load_video('youtube_history.csv')
    tt = load_video('tiktok_history.csv')

    deck = {'generated_for': TODAY, 'followers': FOLLOWERS, 'platforms': {}}

    # --- פייסבוק ואינסטגרם, מהבסיס החודשי ---
    for plat in ('facebook', 'instagram'):
        sub = meta[meta['platform'] == plat].copy()
        sub = sub[sub['month'] <= CUTOFF_MONTH]     # לפני כל חישוב, לא אחריו
        sub['year'] = sub['month'].str[:4]
        block = {'yearly': {}, 'monthly_views': []}
        for y, g in sub.groupby('year'):
            ok = g[g['views_valid']] if 'views_valid' in g else g
            block['yearly'][y] = {
                'posts': int(g['posts'].sum()),
                # הפוסטים שהצפיות באמת מתייחסות אליהם. ב-2024 הצפיות מתחילות
                # בספטמבר, ולהצמיד להן ספירה של שנה שלמה זה לומר שאלפי פוסטים
                # הפיקו את מה שכמה מאות הפיקו.
                'posts_in_views_window': int(ok['posts'].sum()),
                'views_window': (ok['month'].min() if len(ok) else None),
                'views': int(ok['views'].sum(skipna=True) or 0),
                'likes': int(g['likes'].sum(skipna=True) or 0),
                'comments': int(g['comments'].sum(skipna=True) or 0),
                'shares': int(g['shares'].sum(skipna=True) or 0),
            }
            if 'watch_hours' in g:
                block['yearly'][y]['watch_hours'] = int(g['watch_hours'].sum(skipna=True) or 0)
        for _, r in sub[sub['month'] >= VIEWS_FROM].iterrows():
            if r.get('views_valid', True) and pd.notna(r.get('views')):
                block['monthly_views'].append({'month': r['month'], 'views': int(r['views'])})
        # ינואר–יולי בכל שנה. בלי זה משווים שנה שלמה מול שבעה חודשים
        # וקוראים לזה ירידה — וגם התמהיל עצמו נמדד על חלונות שונים.
        ytd = sub[sub['month'].str[5:7] <= '07']
        block['posts_ytd'] = {y: int(g['posts'].sum())
                              for y, g in ytd.groupby('year')}
        mix = {}
        for fmt in ('רילס', 'תמונה', 'קרוסלה', 'וידאו', 'סטטוס', 'לינק'):
            if fmt in ytd.columns:
                per_year = ytd.groupby('year')[fmt].sum()
                if per_year.sum():
                    mix[fmt] = {y: int(v) for y, v in per_year.items()}
        block['format_mix'] = mix
        deck['platforms'][plat] = block

    # --- יוטיוב וטיקטוק ---
    for plat, d in (('youtube', yt), ('tiktok', tt)):
        yearly, ytd = yearly_from_video(d)
        deck['platforms'][plat] = {
            'yearly': yearly, 'ytd': ytd,
            'monthly_views': monthly_from_video(d),
        }

    # ליוטיוב יש מקור טוב יותר מה-API: ייצוא Studio נותן **צפיות בתקופה**
    # ולא "מה שתוכן משנה X צבר עד היום". ההפרש אינו זניח — 642M מול 547M —
    # וכולו הזנב של יוטיוב, שהוא היחיד מהרשתות כאן שיש לו זנב אמיתי.
    deck['platforms']['youtube'].update(_youtube_studio())
    # Shorts מול רגיל — הפילוח היחיד שיש ביוטיוב
    yt_ytd = _ytd(yt)
    deck['platforms']['youtube']['format_mix'] = {
        {'Regular': 'סרטון רגיל', 'Shorts': 'שורטס'}.get(t, t):
            {str(y): int(v) for y, v in g.groupby(g['dt'].dt.year).size().items()}
        for t, g in yt_ytd.groupby('type')}
    for p, d in (('youtube', yt), ('tiktok', tt)):
        for y, g in d.groupby(d['dt'].dt.year):
            blk = deck['platforms'][p]['yearly'].get(str(y))
            if blk:
                blk['posts_in_views_window'] = len(g)
        deck['platforms'][p]['posts_ytd'] = {
            str(y): len(g) for y, g in _ytd(d).groupby(d['dt'].dt.year)}
    # הצפיות וזמן הצפייה מגיעים מ-Studio ומכסים את כל התקופה. רק **ספירת
    # הפריטים** מגיעה מה-API, שחותך את פלייליסט ההעלאות ב-20,000.
    deck['platforms']['youtube']['coverage_note'] = (
        'צפיות וזמן צפייה מייצוא Studio — 953 ימים רצופים, ללא פערים. '
        'ספירת הפריטים מה-API, שחושף 20,000 סרטונים אחרונים (מ-08/2022)')
    deck['platforms']['tiktok']['coverage_note'] = (
        'נמשכו 3,993 מתוך 4,950 סרטונים; ההיסטוריה מתחילה ב-2025-02-17')

    # --- טוויטר: רק מה שהגיליון מכסה ---
    tw = pd.read_csv(os.path.join(PULLED, 'sheet_twitter.csv'), encoding='utf-8-sig')
    tw['dt'] = pd.to_datetime(tw['date'], errors='coerce')
    tw = tw.dropna(subset=['dt'])
    tw = tw[tw['dt'] <= CUTOFF]
    tw['views'] = _num(tw.get('views')).fillna(0)
    deck['platforms']['twitter'] = {
        'from': str(tw['dt'].min().date()), 'to': str(tw['dt'].max().date()),
        'posts': len(tw), 'views': int(tw['views'].sum()),
        'coverage_note': 'ה-API של הספק מגיע 13 יום אחורה בלבד — אין היסטוריה לפני 6/2026',
    }
    deck['platforms']['whatsapp'] = {
        'coverage_note': 'לערוץ אין API. גודל הקהל נמסר ידנית; אין נתוני צפיות',
    }

    # --- מנויי יוטיוב, סדרה יומית ---
    subs = pd.read_csv(os.path.join(SRC, 'youtube_studio', 'subscribers_daily.csv'))
    subs['Date'] = pd.to_datetime(subs['Date'])
    subs = subs[subs['Date'] <= CUTOFF]
    deck['_subs'] = subs
    # החודשים שבהם נוספו הכי הרבה מנויים — לסימון על העקומה. השאלה היא
    # "מתי קפצנו", לא "כמה נוספו אתמול".
    m_subs = subs.groupby(subs['Date'].dt.to_period('M'))['Subscribers'].last()
    m_gain = m_subs.diff().dropna()
    deck['youtube_subscribers'] = {
        'series': [{'month': str(m), 'subs': int(v)} for m, v in m_subs.items()],
        'start': int(subs['Subscribers'].iloc[0]), 'end': int(subs['Subscribers'].iloc[-1]),
        'by_year': {str(y): int(g['Subscribers'].iloc[-1] - g['Subscribers'].iloc[0])
                    for y, g in subs.groupby(subs['Date'].dt.year)},
        'top_months': [{'month': str(m), 'gain': int(v)}
                       for m, v in m_gain.nlargest(3).items()],
    }
    aud = pd.read_csv(os.path.join(SRC, 'youtube_studio', 'monthly_audience.csv'))
    aud['Date'] = pd.to_datetime(aud['Date'])
    aud = aud[aud['Date'] <= CUTOFF]
    deck['youtube_audience'] = {str(y): int(g['Monthly audience'].mean())
                                for y, g in aud.groupby(aud['Date'].dt.year)}

    # --- גיוס עוקבים בפייסבוק (ברוטו — ראו meta_insights/README) ---
    rows = []
    for r in csv.reader(open(os.path.join(SRC, 'meta_insights', 'Follows.csv'),
                             encoding='utf-16')):
        if len(r) == 2 and r[0] not in ('Date', '') and r[1].strip():
            try:
                rows.append((r[0][:10], float(r[1])))   # תאריך מלא: נדרש ליום
            except ValueError:
                pass
    rows = [(d0, v) for d0, v in rows if d0 <= CUTOFF]
    by_month = defaultdict(float)
    for day, v in rows:
        by_month[day[:7]] += v
    fb_monthly = [{'month': m, 'follows': int(v)} for m, v in sorted(by_month.items())]
    # עקומה מצטברת: "כמה עוקבים הצטרפו מתחילת 2024 ועד כל נקודה" — זה מה
    # שהשאלה «בכמה גדלנו» באמת מבקשת, ולא הקצב היומי.
    run = 0
    cum = []
    for m in fb_monthly:
        run += m['follows']
        cum.append({'month': m['month'], 'total': run})
    top_fb = sorted(fb_monthly, key=lambda x: -x['follows'])[:3]
    deck['facebook_follows'] = {
        'monthly': fb_monthly,
        'cumulative': cum,
        'top_months': [{'month': m['month'], 'gain': m['follows']} for m in top_fb],
        'total_gross': run,
        'net_ratio': 0.75,
        'note': ('ברוטו. מול הגיליון על 8 חודשי חפיפה: 136,017 הצטרפויות מול '
                 '102,061 גידול בפועל — אחד מכל ארבעה עוזב'),
    }

    # --- מה שאינסטגרם מלמד על גיוס עוקבים לפי פורמט ---
    deck['instagram_follows_by_format'] = ig_follows_by_format()

    # --- התוכן הגדול של כל שנה ---
    deck['top_content'] = top_content(yt, tt)
    deck['cross_platform'] = cross_platform(deck['top_content'])

    # --- הימים הגדולים, ומה הם עשו לקהל ---
    fb_daily = pd.DataFrame(rows, columns=['date', 'follows'])
    fb_daily['date'] = pd.to_datetime(fb_daily['date'])
    deck['big_days'] = big_days(yt, tt, subs, fb_daily)
    # החלונות מגיעים משכבת הניסוח, ולכן חייבים להתמזג לפני המדידה
    prev_ed = {}
    if os.path.exists(OUT):
        try:
            prev_ed = json.load(open(OUT, encoding='utf-8')).get('editorial', {})
        except (ValueError, OSError):
            prev_ed = {}
    ev_cfg = prev_ed.get('events') or EDITORIAL_SEED['events']
    deck['events'] = measure_events(yt, tt, subs, fb_daily, ev_cfg)
    deck['_candidates'] = candidate_windows(yt, tt)
    deck.pop('_subs', None)

    # סך צפיות בחלון ינואר–יולי, לכל שנה — הבסיס לאחוזי הגידול בשקף השני.
    # רק חלון זהה בכל השנים; אחרת משווים שנה שלמה לשבעה חודשים.
    totals = defaultdict(int)
    for plat, blk in deck['platforms'].items():
        for m in blk.get('monthly_views', []):
            if m['month'][5:7] <= '07':
                totals[m['month'][:4]] += m['views']
    deck['views_by_year_ytd'] = dict(sorted(totals.items()))

    # תיאור לכל קפיצה שמסומנת על העקומות — מאותה רשת, ולא כטענת סיבתיות
    for block, plat in ((deck['youtube_subscribers'], 'youtube'),
                        (deck['facebook_follows'], 'facebook')):
        heads = month_headlines(yt, tt, plat)
        for m in block.get('top_months', []):
            h = heads.get(m['month']) or {}
            m['headline'] = h.get('title', '')
            m['headline_platform'] = h.get('platform', '')

    return deck


def _youtube_studio():
    """צפיות, זמן צפייה ופילוח סוגי תוכן מייצוא Studio — 953 ימים, בלי פערים."""
    ys = os.path.join(SRC, 'youtube_studio')
    d = pd.read_csv(os.path.join(ys, 'daily_views.csv'))
    d['Date'] = pd.to_datetime(d['Date'])
    d = d[d['Date'] <= CUTOFF]

    tot = pd.read_csv(os.path.join(ys, 'type_totals.csv'))
    by_type = {}
    for _, r in tot.iterrows():
        name = {'Videos': 'סרטונים', 'Shorts': 'שורטס',
                'Live stream': 'שידור חי'}.get(r['Content type'])
        if not name:
            continue
        by_type[name] = {
            'views': int(r['Views']),
            'watch_hours': int(float(r['Watch time (hours)'])),
            'avg_duration': str(r['Average view duration']),
            'pct_viewed': float(r['Average percentage viewed (%)']),
        }
    total_watch = int(float(tot[tot['Content type'] == 'Total']
                            ['Watch time (hours)'].iloc[0]))

    bt = pd.read_csv(os.path.join(ys, 'daily_by_type.csv'))
    bt['Date'] = pd.to_datetime(bt['Date'])
    bt = bt[bt['Date'] <= CUTOFF]
    heb = {'Videos': 'סרטונים', 'Shorts': 'שורטס', 'Live stream': 'שידור חי'}
    mix = {}
    for t, g in bt.groupby('Content type'):
        if t in heb:
            mix[heb[t]] = {str(y): int(v) for y, v in
                           g.groupby(g['Date'].dt.year)['Views'].sum().items()}

    yearly = {}
    for y, g in d.groupby(d['Date'].dt.year):
        yearly[str(y)] = {'views': int(g['Views'].sum()), 'days': len(g)}

    return {
        'yearly_period': yearly,
        'views_total': int(d['Views'].sum()),
        'watch_hours': total_watch,
        'by_type': by_type,
        'views_mix': mix,
        'monthly_views': [{'month': str(m), 'views': int(v)} for m, v in
                          d.groupby(d['Date'].dt.to_period('M'))['Views'].sum().items()
                          if VIEWS_FROM <= str(m) <= CUTOFF_MONTH],
        'source_note': ('צפיות בתקופה מייצוא YouTube Studio — לא "מה שתוכן '
                        'השנה צבר עד היום"'),
    }


def ig_follows_by_format():
    """עוקבים לכל 1,000 צפיות — המספר שמראה מה באמת מגייס."""
    files = [os.path.join(SRC, 'Jan-01-2024_Dec-31-2024_4588672571364735.csv'),
             os.path.join(SRC, 'data_buisness_suit', '2025 חוץ מסטוריז.xlsx'),
             os.path.join(SRC, 'data_buisness_suit',
                          'Jan-01-2026_Aug-10-2026_1071135865260092.csv')]
    parts = []
    for f in files:
        d = pd.read_excel(f) if f.endswith('.xlsx') else pd.read_csv(f, encoding='utf-8-sig')
        d = d[d['Account username'] == 'kan_news'].copy()
        d['dt'] = pd.to_datetime(d['Publish time'], format='mixed', errors='coerce')
        for c in ('Views', 'Follows'):
            d[c] = _num(d[c])
        parts.append(d[['dt', 'Post type', 'Views', 'Follows']])
    d = pd.concat(parts, ignore_index=True)
    d = d[(d['dt'] >= VIEWS_FROM) & (d['dt'] <= CUTOFF)]  # לפני כן אינן תקפות
    d['fmt'] = d['Post type'].map({'IG reel': 'רילס', 'IG image': 'תמונה',
                                   'IG carousel': 'קרוסלה', 'IGTV': 'רילס'})
    out = {}
    for fmt, g in d.groupby('fmt'):
        v, f = g['Views'].sum(), g['Follows'].sum()
        out[fmt] = {'posts': len(g), 'views': int(v), 'follows': int(f),
                    'per_1k_views': round(f / v * 1000, 2) if v else 0}
    return out


def all_items(yt, tt):
    """כל הפריטים מכל הרשתות בטבלה אחת: תאריך, צפיות, כותרת, רשת."""
    frames = []
    ig_files = [os.path.join(SRC, 'Jan-01-2024_Dec-31-2024_4588672571364735.csv'),
                os.path.join(SRC, 'data_buisness_suit', '2025 חוץ מסטוריז.xlsx'),
                os.path.join(SRC, 'data_buisness_suit',
                             'Jan-01-2026_Aug-10-2026_1071135865260092.csv')]
    for f in ig_files:
        d = pd.read_excel(f) if f.endswith('.xlsx') else pd.read_csv(f, encoding='utf-8-sig')
        d = d[d['Account username'] == 'kan_news']
        frames.append(pd.DataFrame({
            'dt': pd.to_datetime(d['Publish time'], format='mixed', errors='coerce'),
            'views': _num(d['Views']), 'title': d['Description'].map(clean_title),
            'platform': 'instagram'}))

    fb25 = pd.read_csv(os.path.join(PULLED, 'fb_2025_metrics.csv'), encoding='utf-8-sig')
    frames.append(pd.DataFrame({
        'dt': pd.to_datetime(fb25['date'], errors='coerce'),
        'views': _num(fb25['views']), 'title': '', 'platform': 'facebook'}))
    for f in ('Jan-01-2024_Dec-31-2024_1509169891251840.csv',
              'Jan-01-2026_Aug-11-2026_1369894801784288.csv'):
        e = pd.read_csv(os.path.join(SRC, f), encoding='utf-8-sig', low_memory=False)
        e = e[e['Page ID'].astype(str) == '100064467291406']
        frames.append(pd.DataFrame({
            'dt': pd.to_datetime(e['Publish time'], format='mixed', errors='coerce'),
            'views': _num(e['Views']), 'title': e['Description'].map(clean_title),
            'platform': 'facebook'}))
    for d, name in ((yt, 'youtube'), (tt, 'tiktok')):
        frames.append(pd.DataFrame({
            'dt': d['dt'], 'views': d['views'],
            'title': d['title'].map(clean_title), 'platform': name}))

    a = pd.concat(frames, ignore_index=True).dropna(subset=['dt', 'views'])
    return a[(a['dt'] >= VIEWS_FROM) & (a['dt'] <= CUTOFF)]


def month_headlines(yt, tt, platform=None):
    """לכל חודש — הפריט הגדול ביותר שפורסם בו, ברשת המבוקשת.

    **זה תיאור, לא הסבר.** הפריט הגדול בחודש אינו בהכרח מה שגרם לאנשים
    להירשם, ולכן הוא מוצג כ"התוכן הגדול באותו חודש" ולא כסיבה. הסינון לפי
    רשת הוא המינימום: לסמן קפיצה במנויי יוטיוב ולהצמיד לה פוסט מטיקטוק היה
    שגוי פעמיים.
    """
    a = all_items(yt, tt)
    a = a[a['title'].astype(str).str.len() > 12]
    a['m'] = a['dt'].dt.to_period('M').astype(str)

    def _top(frame):
        out = {}
        for m, g in frame.groupby('m'):
            r = g.nlargest(1, 'views').iloc[0]
            out[m] = {'title': str(r['title']), 'platform': str(r['platform'])}
        return out

    everywhere = _top(a)
    if not platform:
        return everywhere
    # לפייסבוק 2025 אין טקסט פוסטים (הבקפיל מה-API לא שומר אותו), ולכן
    # נופלים לפריט הגדול בכל הרשתות — אבל **מציינים מאיזו רשת הוא**, כדי
    # שלא ייראה כאילו הוא של הרשת שהעקומה מתארת.
    own = _top(a[a['platform'] == platform])
    return {m: own.get(m) or everywhere.get(m) for m in everywhere}


def measure_events(yt, tt, subs, fb_follows, events):
    """מודד חלונות שנקבעו ביד. **התאריכים אינם מתגלים מהנתונים — הם נקבעים.**

    הניסיון הקודם איתר חלונות אוטומטית (רצפים מעל פי 2 מהחציון) והוא טעה
    בדיוק במקום שבו זה חשוב: הוא פתח את מלחמת יוני 2025 ב-10.6 כי באותו יום
    היה קליפ ויראלי על בריחה משוטר, בזמן שהמבצע התחיל ב-13.6. אלגוריתם מוצא
    שיאים; רק אדם יודע מה האירוע. לכן החלונות מגיעים מ-`editorial.events`,
    והסקריפט רק מודד אותם.

    `candidate_windows()` נשאר ככלי גילוי — להצעת חלונות שטרם נבדקו — אבל
    שום דבר במצגת לא מסתמך עליו.
    """
    a = all_items(yt, tt)
    a['d'] = a['dt'].dt.date
    day = a.groupby('d')['views'].sum().sort_index()
    med = float(day.median())
    sub_gain = subs.set_index(subs['Date'].dt.date)['Subscribers'].diff()
    fb_gain = fb_follows.set_index(fb_follows['date'].dt.date)['follows']

    out = []
    for key, meta in (events or {}).items():
        if not meta.get('show', True):
            continue
        s = pd.to_datetime(meta.get('from', key)).date()
        e = pd.to_datetime(meta.get('to', key)).date()
        win = day[(day.index >= s) & (day.index <= e)]
        if not len(win):
            continue
        days = (e - s).days + 1
        g = a[(a['d'] >= s) & (a['d'] <= e)]
        named = g[g['title'].astype(str).str.len() > 12]
        tops = (named if len(named) else g).nlargest(3, 'views')
        peak = win.idxmax()
        out.append({
            'key': key, 'name': meta.get('name', ''),
            'from': str(s), 'to': str(e), 'days': days,
            'views': int(win.sum()),
            'vs_typical': round(float(win.sum()) / days / med, 1),
            'peak_date': str(peak), 'peak_x': round(float(win.max()) / med, 1),
            'yt_subs': int(sub_gain[(sub_gain.index >= s) & (sub_gain.index <= e)].sum() or 0),
            'fb_follows': int(fb_gain[(fb_gain.index >= s) & (fb_gain.index <= e)].sum() or 0),
            'headlines': [str(r['title']) for _, r in tops.iterrows()],
        })
    out.sort(key=lambda x: -x['views'])
    return {'typical_day': int(med), 'windows': out}


def candidate_windows(yt, tt, floor=40_000_000):
    """כלי גילוי בלבד: רצפים חשודים לאירוע, להצגה בפלט הריצה.

    לא נכנס למצגת — ראו ההסבר ב-`measure_events`.
    """
    a = all_items(yt, tt)
    a['d'] = a['dt'].dt.date
    day = a.groupby('d')['views'].sum().sort_index()
    med = float(day.median())
    runs, cur = [], None
    for d in day[day >= 2 * med].index:
        if cur and (d - cur[1]).days <= 4:
            cur = (cur[0], d)
        else:
            if cur:
                runs.append(cur)
            cur = (d, d)
    if cur:
        runs.append(cur)
    out = []
    for s, e in runs:
        win = day[(day.index >= s) & (day.index <= e)]
        if win.sum() < floor:
            continue
        g = a[(a['d'] >= s) & (a['d'] <= e)]
        named = g[g['title'].astype(str).str.len() > 12]
        top = (named if len(named) else g).nlargest(1, 'views')
        out.append((int(win.sum()), str(s), str(e),
                    str(top.iloc[0]['title'])[:60] if len(top) else ''))
    return sorted(out, reverse=True)


def big_days(yt, tt, subs, fb_follows, n=6):
    """הימים שבהם התוכן עשה הכי הרבה — ומה קרה בהם.

    לא מביאים ציר זמן של אירועים מבחוץ: **הנתונים מוצאים את היום והתוכן נותן
    לו שם.** הכותרת של הפריט הגדול באותו יום היא התיאור המדויק ביותר של מה
    שקרה, והיא גם מה שבאמת פורסם — בלי פרשנות שלנו.

    לצד הצפיות נמדד גם מה האירוע עשה לקהל: מנויי יוטיוב שנוספו והצטרפויות
    לפייסבוק *באותו יום*.
    """
    a = all_items(yt, tt)
    a['day'] = a['dt'].dt.date
    per_day = a.groupby('day')['views'].sum()
    typical = float(per_day.median())

    sub_gain = subs.set_index(subs['Date'].dt.date)['Subscribers'].diff()
    fb_gain = fb_follows.set_index(fb_follows['date'].dt.date)['follows']

    out = []
    for day, total in per_day.nlargest(n).items():
        g = a[a['day'] == day]
        named = g[g['title'].astype(str).str.len() > 12]
        top = (named if len(named) else g).nlargest(1, 'views').iloc[0]
        out.append({
            'date': str(day),
            'views': int(total),
            'vs_typical': round(total / typical, 1) if typical else 0,
            'headline': str(top['title']),
            'top_platform': top['platform'],
            'top_views': int(top['views']),
            'yt_subs': int(sub_gain.get(day, 0) or 0),
            'fb_follows': int(fb_gain.get(day, 0) or 0),
        })
    return {'typical_day': int(typical), 'days': out}


def top_content(yt, tt):
    """הפריט הגדול של כל שנה בכל רשת. מספטמבר 2024 בלבד."""
    out = defaultdict(list)

    ig_files = [os.path.join(SRC, 'Jan-01-2024_Dec-31-2024_4588672571364735.csv'),
                os.path.join(SRC, 'data_buisness_suit', '2025 חוץ מסטוריז.xlsx'),
                os.path.join(SRC, 'data_buisness_suit',
                             'Jan-01-2026_Aug-10-2026_1071135865260092.csv')]
    parts = []
    for f in ig_files:
        d = pd.read_excel(f) if f.endswith('.xlsx') else pd.read_csv(f, encoding='utf-8-sig')
        d = d[d['Account username'] == 'kan_news'].copy()
        d['dt'] = pd.to_datetime(d['Publish time'], format='mixed', errors='coerce')
        d['views'] = _num(d['Views'])
        d['title'] = d['Description'].map(clean_title)
        parts.append(d[['dt', 'views', 'title']])
    ig = pd.concat(parts, ignore_index=True)

    # 2025 מהבקפיל — אין בו טקסט הפוסט, ולכן הכותרת נשארת ריקה ונמסרת
    # לשכבת הניסוח; ה-permalink הוא מה שמאפשר למלא אותה בלי לנחש.
    fb25 = pd.read_csv(os.path.join(PULLED, 'fb_2025_metrics.csv'), encoding='utf-8-sig')
    fb25['dt'] = pd.to_datetime(fb25['date'], errors='coerce')
    fb25['views'] = _num(fb25['views'])
    fb25['title'] = ''
    fb25['link'] = fb25['permalink']

    fb_exports = []
    for f in ('Jan-01-2024_Dec-31-2024_1509169891251840.csv',
              'Jan-01-2026_Aug-11-2026_1369894801784288.csv'):
        e = pd.read_csv(os.path.join(SRC, f), encoding='utf-8-sig', low_memory=False)
        e = e[e['Page ID'].astype(str) == '100064467291406'].copy()
        e['dt'] = pd.to_datetime(e['Publish time'], format='mixed', errors='coerce')
        e['views'] = _num(e['Views'])
        # ב-2024 ה-Title לרוב ריק והטקסט יושב ב-Description
        e['title'] = (e['Title'].fillna('').astype(str).str.strip()
                      .where(lambda s: s.str.len() > 0,
                             e['Description'].fillna('').astype(str))
                      .map(clean_title))
        e['link'] = e['Permalink']
        fb_exports.append(e[['dt', 'views', 'title', 'link']])

    fb = pd.concat([fb25[['dt', 'views', 'title', 'link']]] + fb_exports,
                   ignore_index=True)
    for d in (ig, yt, tt):
        if 'link' not in d.columns:
            d['link'] = ''

    for name, d in (('instagram', ig), ('facebook', fb),
                    ('youtube', yt[['dt', 'views', 'title', 'link']]),
                    ('tiktok', tt[['dt', 'views', 'title', 'link']])):
        d = d[(d['dt'] >= VIEWS_FROM) & (d['dt'] <= CUTOFF) & d['views'].notna()]
        for y, g in d.groupby(d['dt'].dt.year):
            r = g.nlargest(1, 'views').iloc[0]
            out[str(y)].append({'platform': name, 'views': int(r['views']),
                                'title': clean_title(r['title']),
                                'link': str(r.get('link', '') or ''),
                                'date': str(r['dt'].date())})
    out = {y: sorted(v, key=lambda x: -x['views']) for y, v in sorted(out.items())}
    return out


def _tokens(s):
    """מילים משמעותיות בלבד — מילות קישור קצרות יוצרות התאמות שווא."""
    bad = {'של', 'עם', 'את', 'על', 'לא', 'זה', 'הוא', 'היא', 'כי', 'גם'}
    return {w.strip('.,:;"\'!?()״׳-') for w in str(s).split()
            if len(w) > 2 and w not in bad}


def cross_platform(top):
    """אותו סיפור שהגיע לראש בכמה רשתות באותה שנה.

    הכותרות לא זהות בין הרשתות (כל רשת נערכת בנפרד), ולכן ההשוואה היא לפי
    חפיפת מילים ולא לפי טקסט מדויק. 0.5 — מתחת לזה נדבקים סיפורים שרק חולקים
    נושא.
    """
    res = {}
    for y, items in top.items():
        best = None
        for i, a in enumerate(items):
            for b in items[i + 1:]:
                ta, tb = _tokens(a['title']), _tokens(b['title'])
                if not ta or not tb:
                    continue
                overlap = len(ta & tb) / min(len(ta), len(tb))
                if overlap >= 0.5:
                    grp = [a, b]
                    tot = sum(x['views'] for x in grp)
                    if not best or tot > best['views']:
                        best = {'title': max(grp, key=lambda x: len(x['title']))['title'],
                                'views': tot,
                                'platforms': [x['platform'] for x in grp]}
        if best:
            res[y] = best
    return res


EDITORIAL_SEED = {
    "cover_title": "הסושיאל של כאן חדשות",
    "cover_subtitle": "ינואר 2024 – יולי 2026",
    "slides": {},
    # אירועים חדשותיים. **התאריכים כאן קובעים** — הסקריפט רק מודד אותם.
    # שמות ותאריכי המבצעים אומתו מול מקורות חיצוניים, ולא נגזרו מהכותרות:
    # "עם כלביא" 13–24.6.2025 (מלחמת 12 הימים), "שאגת הארי" מ-28.2.2026.
    # להוסיף אירוע: מפתח כלשהו עם from/to/name. `show: false` מסתיר.
    "events": {
        "am_kelavia": {"name": "מבצע «עם כלביא» — מלחמת 12 הימים",
                       "from": "2025-06-13", "to": "2025-06-24", "show": True},
        "hostages_end": {"name": "שחרור החטופים וסיום מלחמת חרבות ברזל",
                         "from": "2025-10-09", "to": "2025-10-20", "show": True},
        "shaagat_haari": {"name": "מבצע «שאגת הארי»",
                          "from": "2026-02-28", "to": "2026-03-31", "show": True},
        "bibas": {"name": "החזרת חללי משפחת ביבס ושחרור אברה מנגיסטו",
                  "from": "2025-02-20", "to": "2025-02-27", "show": True},
    },
    "_how_to_edit": ("כל טקסט כאן נכתב ביד ושורד הרצה חוזרת של הסקריפט. "
                     "המספרים מתעדכנים מהנתונים; המשפטים לא."),
}


def _merge_editorial(deck):
    """שכבת הניסוח לא נדרסת. זה הלקח מ-weekly_deck, ששטף אותה פעמיים."""
    prev = {}
    if os.path.exists(OUT):
        try:
            prev = json.load(open(OUT, encoding='utf-8')).get('editorial', {})
        except (ValueError, OSError):
            prev = {}
    ed = dict(EDITORIAL_SEED)
    ed.update(prev)
    deck['editorial'] = ed
    return deck


def main():
    deck = _merge_editorial(build())
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(deck, f, ensure_ascii=False, indent=2)

    print("נכתב %s" % OUT)
    print("\nעוקבים: %s בסך הכל" % format(sum(FOLLOWERS.values()), ','))
    for p, n in sorted(FOLLOWERS.items(), key=lambda x: -x[1]):
        print("   %-10s %10s" % (p, format(n, ',')))
    print("\nצפיות לפי שנה (תוכן שפורסם באותה שנה, מצטבר עד היום):")
    for p in ('facebook', 'instagram', 'youtube', 'tiktok'):
        y = deck['platforms'][p].get('yearly', {})
        parts = ["%s %s" % (k, format(v['views'], ',')) for k, v in sorted(y.items())
                 if v.get('views')]
        print("   %-10s %s" % (p, ' | '.join(parts)))
    print("\nיוטיוב מנויים: %s → %s" % (
        format(deck['youtube_subscribers']['start'], ','),
        format(deck['youtube_subscribers']['end'], ',')))
    cands = deck.pop('_candidates', [])
    if cands:
        print("\nחלונות מועמדים שאותרו בנתונים (לא נכנסים למצגת — לבדיקה בלבד):")
        for tot, s0, e0, ttl in cands[:8]:
            mark = '' if any(w['from'] <= e0 and w['to'] >= s0
                             for w in deck['events']['windows']) else '  ← לא מכוסה'
            print("   %s → %s  %14s%s" % (s0, e0, format(tot, ','), mark))
            print("      %s" % ttl)
    print("\nאירועים שנמדדו:")
    for w in deck['events']['windows']:
        print("   %-46s %s → %s  %14s  שיא x%.1f ב-%s"
              % (w['name'], w['from'], w['to'], format(w['views'], ','),
                 w['peak_x'], w['peak_date']))
    print()
    print("אינסטגרם, עוקבים ל-1000 צפיות לפי פורמט:")
    for fmt, v in sorted(deck['instagram_follows_by_format'].items(),
                         key=lambda x: -x[1]['per_1k_views']):
        print("   %-8s %5.2f  (%s פוסטים)" % (fmt, v['per_1k_views'], format(v['posts'], ',')))


if __name__ == '__main__':
    sys.exit(main())
